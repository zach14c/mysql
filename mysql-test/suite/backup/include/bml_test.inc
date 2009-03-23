# This is a "subroutine" for backup_bml test. It executes DDL
# statements given by variables $DDL1-4 in parallel with BACKUP/RESTORE 
# statement and checks that they correctly block each other.
#
# If $backup_to has value, then BACKUP operation is performed and backup image 
# is stored at location $backup_to.
#
# If $restore_from has value, then RESTORE operation from the given image is 
# performed.
#
# To use this file the following conditions must be satisfied:
# - five connections con1-5 must be set up,
# - database bml_test must exist,
# - procedure test.check_results(), used to check effects of DDLs must 
#   be defined,
# - If $restore_from is set, it must point at valid backup image.
#
# It is responsibility of the user of this file to populate bml_test
# database with data prior to using it.
#
# Backup test sequence diagram (not UML)
# --------------------------------------
# Four DDL statements and a backup command are specified.
# The first two DDL statements are run as "in progress" when
# the backup starts. The other two DDL statements are run
# when the backup or restore is already running. Tests results
# show the realization of the assumptions above.
#
#     con5      con1     con2     con3     con4   default
#    (setup)     |        |        |        |        |
#       |        |        |        |        |        |
#       |        |        |        |        |     check A
#       |       DDL1      |        |        |        |
#       |        *       DDL2      |        |        |
#       |        *        *        |        |        |
# BACKUP/RESTORE *        *        |        |        |
#       *        *        *        |        |        |
#       *        *        *        |        |     check B
#       *        =        =        |        |        |
#       *        |        |        |        |     check C
#       *        |        |       DDL3      |        |
#       *        |        |        *       DDL4      |
#       *        |        |        *        *     check D
#       =        |        |        *        *        |
#       |        |        |        =        =        |
#       |        |        |        |        |     check E
#
# Notes: Symbol '*' denotes execution of a statement, symbol '=' its 
# termination.
#
# During test, the following checks are made:
#
# Checkpoint A) To see the initial situation.
# Checkpoint B) BACKUP/RESTORE is blocked.
# Checkpoint C) DDL1 and DDL2 completed and BACKUP/RESTORE started its job.
# Checkpoint D) DDL3 and DDL4 are blocked.
# Checkpoint E) BACKUP/RESTORE and all DDLs completed.
#
# Note by Ingo: I wonder if the line above "(results)" is correct.
# I understand the above explanations so that DDL3 and 4 cannot
# resume before BACKUP/RESTORE finished. But the line indicates
# that these can resume in any order.

if (`select '$backup_to' <> ''`)
{
  let $operation= BACKUP DATABASE bml_test TO '$backup_to';
}

if (`select '$restore_from' <> ''`)
{
  let $operation= RESTORE FROM '$restore_from' OVERWRITE;
}

--echo
--echo ########################################################
--echo # Running BML test for the following statements:
--echo #
--echo # DDL1= $DDL1
--echo # DDL2= $DDL2
--echo # DDL3= $DDL3
--echo # DDL4= $DDL4
--echo #
--echo # BML is activated by $operation.
--echo ########################################################
--echo

connection default;
SET DEBUG_SYNC= 'reset';
PURGE BACKUP LOGS;

#
# Make bml_test the default database in all connections.
#
USE bml_test;
connection con1;
SET DEBUG_SYNC= 'reset';
USE bml_test;
connection con2;
SET DEBUG_SYNC= 'reset';
USE bml_test;
connection con3;
SET DEBUG_SYNC= 'reset';
USE bml_test;
connection con4;
SET DEBUG_SYNC= 'reset';
USE bml_test;
connection con5;
SET DEBUG_SYNC= 'reset';
USE bml_test;

--echo
--echo == Checkpoint A ==
--echo # Check the initial situation.
CALL test.check_results();
--echo ==================
--echo

# Start DDL1 and DDL2

 connection con1;
 --echo # con1: Start DDL1 making it to stop after it has started (and 
 --echo #       possibly registered with BML).
 SET DEBUG_SYNC= 'before_execute_sql_command SIGNAL ddl1_started 
   					       WAIT_FOR continue_ddl';
 --send
 eval $DDL1;

   connection con2;
   --echo # con2: Wait for DDL1 to reach its synchronization point.
   SET DEBUG_SYNC= 'now WAIT_FOR ddl1_started TIMEOUT 15';
   --echo # con2: Start DDL2 making it to stop after it has started (and 
   --echo #       possibly registered with BML).
   SET DEBUG_SYNC= 'before_execute_sql_command SIGNAL ddl2_started 
   					       WAIT_FOR continue_ddl';
   --send
   eval $DDL2;

# Start BACKUP/RESTORE
 
         connection con5;
         --echo # con5: Wait for DDL2 to reach its synchronization point.
         SET DEBUG_SYNC= 'now WAIT_FOR ddl2_started TIMEOUT 15';
         # To be safe clear before_execute_sql_command which is hit by
         # every executed statement
         SET DEBUG_SYNC='before_execute_sql_command CLEAR';

         --echo # con5: Activate synchronization points for BACKUP/RESTORE.
         # 
         # Make BACKUP/RESTORE to operation which should be blocked by ongoing
         # statements. Make it send 'bup_waiting' signal just before it checks
         # for registered statements inside BML. 
         #
         SET DEBUG_SYNC= 'bml_get_check2 SIGNAL bup_waiting';
         #
         # Whether BACKUP/RESTORE is blocked or not make it stop after it 
         # completes preparations, including activating the BML.
         #
         SET DEBUG_SYNC= 'after_backup_start_backup  WAIT_FOR continue_bup';
         SET DEBUG_SYNC= 'after_backup_start_restore WAIT_FOR continue_bup';
	 #
	 # Also make it to stop in the middle of operation after seeing 
	 # 'continue_bup' so that DDL3 and DDL4 can be started in parallel
	 #
	 SET DEBUG_SYNC= 'before_backup_meta SIGNAL bup_running WAIT_FOR finish_bup';
	 SET DEBUG_SYNC= 'start_do_restore SIGNAL bup_running WAIT_FOR finish_bup';	 

         --echo # con5: Starting BACKUP/RESTORE operation -- should be blocked 
         --echo #       by ongoing DDLs.
         # arrange for deterministic backup_id = 500
         SET SESSION debug="+d,set_backup_id";
         --send
         eval $operation;
 
connection default;
--echo # Waiting for BACKUP/RESTORE to reach its synchronization point.
SET DEBUG_SYNC= 'now WAIT_FOR bup_waiting TIMEOUT 15';

--echo
--echo == Checkpoint B ==
--echo #
--echo # DDL1= $DDL1
--echo # DDL2= $DDL2
--echo # DDL3= $DDL3
--echo # DDL4= $DDL4
--echo #
eval SELECT state, info FROM INFORMATION_SCHEMA.PROCESSLIST
  WHERE info LIKE "BACKUP DATABASE%"
     OR info LIKE "RESTORE FROM%"
     OR info LIKE "$DDL1%"
     OR info LIKE "$DDL2%";
--echo # Checking that BACKUP is blocked by DDLs.
SELECT object, notes, error_num FROM mysql.backup_progress WHERE backup_id=500;
--echo # Checking that DDL1 and DDL2 have not executed.
CALL test.check_results();
--echo ==================
--echo

--echo # Resume DDL1 and DDL2. 
--echo # Now BACKUP/RESTORE should stop after acquiring BML.
SET DEBUG_SYNC= 'now SIGNAL continue_ddl';

 connection con1;
 --echo # con1: Reaping DDL1
 reap;

   connection con2;
   --echo # con2: Reaping DDL2
   reap;

connection default;
# Ensure that BACKUP/RESTORE is in progress, after activating BML.
SET DEBUG_SYNC= 'now SIGNAL continue_bup WAIT_FOR bup_running TIMEOUT 15';

--echo
--echo == Checkpoint C ==
--echo #
--echo # DDL1= $DDL1
--echo # DDL2= $DDL2
--echo # DDL3= $DDL3
--echo # DDL4= $DDL4
--echo #
SELECT state, info FROM INFORMATION_SCHEMA.PROCESSLIST
  WHERE info LIKE "BACKUP DATABASE%"
     OR info LIKE "RESTORE FROM%";
--echo # Checking that DDL1 and DDL2 have executed.
CALL test.check_results();
--echo ==================
--echo

# Start DDL3 and DDL4 while BML is taken.

     connection con3;
     --echo # con3: Send DDL3 but it is blocked by BACKUP/RESTORE 
     --echo #       (will not be in backup) make it send signal when
     --echo #       blocked on BML.
     SET DEBUG_SYNC= 'bml_enter_check SIGNAL ddl3_blocked';
     --send
     eval $DDL3;

       connection con4;
       --echo # Wait for DDL3 to send its signal.
       SET DEBUG_SYNC= 'now WAIT_FOR ddl3_blocked TIMEOUT 15';
       --echo # con3: Send DDL3 but it is blocked by BACKUP/RESTORE 
       --echo #       (will not be in backup) make it send signal when
       --echo #       blocked on BML.
       SET DEBUG_SYNC= 'bml_enter_check SIGNAL ddl4_blocked';
       --send
       eval $DDL4;

# check that DDL3 and DDL4 are blocked

connection default;
--echo # Wait for DDL4 to send its signal.
SET DEBUG_SYNC= 'now WAIT_FOR ddl4_blocked TIMEOUT 15';

--echo
--echo == Checkpoint D ==
--echo #
--echo # DDL1= $DDL1
--echo # DDL2= $DDL2
--echo # DDL3= $DDL3
--echo # DDL4= $DDL4
--echo #
eval SELECT state, info FROM INFORMATION_SCHEMA.PROCESSLIST
  WHERE info LIKE "$DDL3%"
     OR info LIKE "$DDL4%";
--echo # Checking that DDL3 and DDL4 have not executed.
CALL test.check_results();
--echo ==================
--echo

--echo # Resume BACKUP/RESTORE - this allows DDL3 and DDL4 to complete.
SET DEBUG_SYNC= 'now SIGNAL finish_bup';

         connection con5;
         --echo # con5: Reaping BACKUP/RESTORE
         reap;

     connection con3;
     --echo # con3: Completing DDL3
     reap;

       connection con4;
       --echo # con4: Completing DDL4
       reap;

connection default;

--echo
--echo == Checkpoint E ==
--echo #
--echo # DDL1= $DDL1
--echo # DDL2= $DDL2
--echo # DDL3= $DDL3
--echo # DDL4= $DDL4
--echo #
--echo # Checking that DDL3 and DDL4 have executed.
CALL test.check_results();
--echo ==================
--echo

# Cleanup

connection con1;
SET DEBUG_SYNC= 'reset';
connection con2;
SET DEBUG_SYNC= 'reset';
connection con3;
SET DEBUG_SYNC= 'reset';
connection con4;
SET DEBUG_SYNC= 'reset';
connection con5;
SET DEBUG_SYNC= 'reset';
connection default;
SET DEBUG_SYNC= 'reset';

let $backup_to=;
let $restore_from=;

connection con5;
  SET debug="-d";


