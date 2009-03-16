#
# This is a "subroutine" for backup_interrupt.test. It performs the following:
#
# 1. Start BACKUP or RESTORE statement and use a synchronization point
#    to stop it in the middle of execution.
# 2. In another connection, wait for the statement to reach the synchronization
#    point, and then KILL it.
# 3. Signal the stopped BACKUP/RESTORE statement so that it resumes execution
#    after being killed.
# 4. Check the error response from the statement, contents of the error stack
#    and backup logs.
#
# Variable $do_restore determines whether BACKUP or RESTORE is executed. The
# synchronization point to use is stored in $sync_point. 
#
# This script uses the following assumptions, which must be satisfied by 
# its user:
#
# - $bdir holds the current value of @@backupdir variable.
# - There is a second connection named 'killer'.
# - There exists database called 'bup_intr'.
# - If $do_restore = 1 then $bdir/bup_intr.bkp should contain valid backup 
#   image.
#

let $command= BACKUP DATABASE bup_intr TO 'bup_intr.bkp';

if ($do_restore)
{
  let $command= RESTORE FROM 'bup_intr.bkp';
}

--echo 
--echo ######################################################################
--echo #
--echo # Testing interruption of command $command
--echo # at synchronization point $sync_point.
--echo #
--echo ######################################################################
--echo 
--connection default

let $id=`select connection_id()`;

PURGE BACKUP LOGS;
SET DEBUG_SYNC='reset';
eval SET DEBUG_SYNC='$sync_point SIGNAL here WAIT_FOR go';
#
# Arrange for 'here' signal to be always sent at the end of BACKUP/RESTORE 
# operation (in the destructor of backup/restore context class). This way
# test will not hang waiting for the signal even if $sync_point is invalid 
# or never hit. The fact that we missed the requested synchronization point
# will be detected with a SELECT from I_S.PROCESSLIST (see below). Also,
# the reply from BACKUP/RESTORE will not be the expected ER_QUERY_INTERRUPTED
# error in most cases.
#
SET DEBUG_SYNC='backup_restore_ctx_dtor SIGNAL here';

--echo #
--echo # Start the command.
--echo #
--send
eval $command;

  --connection killer

  --echo #
  --echo # Wait for the command to reach its synchronization point,
  --echo # then kill it.
  --echo #
  SET DEBUG_SYNC='now WAIT_FOR here';
  --replace_regex /id=[0-9]+/id=<query id>/
  eval SELECT state FROM INFORMATION_SCHEMA.PROCESSLIST WHERE id=$id;
  --replace_regex /QUERY [0-9]+/QUERY <query id>/
  eval KILL QUERY $id;
  SET DEBUG_SYNC='now SIGNAL go';

--connection default

--echo #
--echo # Reap the command and show results.
--echo #
--error ER_QUERY_INTERRUPTED
reap;
--replace_column 2 <error-code>
# One error message contains file path - mask it out.
--replace_regex /Error on delete of '.*'/Error on delete of <backup image path>/
SHOW WARNINGS;

--echo #
--echo # Examine backup logs.
--echo #
--echo # FIXME: Until BUG#39924 is fixed, change to BUP_CANCEL state will not be
--echo # seen in backup_progress table and backup_history table will be empty.
--echo # When the bug is fixed the output below will change and the result file
--echo # should be modifed accordingly.
--echo #
SELECT object, error_num, notes FROM mysql.backup_progress;
query_vertical SELECT * FROM mysql.backup_history;

# check that backup image file was removed
if (!$do_restore)
{
  --error 1
  --remove_file $bdir/bup_intr.bkp
}
