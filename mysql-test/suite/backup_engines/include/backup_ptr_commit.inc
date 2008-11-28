##########################################################################
# Author: Hema
# Date: 2008-07-30
# Purpose: Test to ensure that point in time recovery includes committed 
# transactions in the backup. In this test we will commit the data during 
# backup. This can be accomplished by using debug_sync points.
###############################################################################


--source include/have_debug_sync.inc
--source include/have_innodb.inc
--source include/not_embedded.inc
--source include/have_log_bin.inc
SELECT @@BINLOG_FORMAT;
RESET MASTER;

--error 0,1
--remove_file $MYSQLTEST_VARDIR/master-data/ptr_objects.bak;

connect (con1,localhost,root,,);
connect (con2,localhost,root,,);
connect (con3,localhost,root,,);

connection con1;

--echo **START TEST**
--echo *** TEST1 ***
--echo This test will recover data that were committed during backup
--echo operation. This is acheived by means of debug_sync points
--echo which pauses the backup database operation during commit.
--echo

#We set constant timestamp inside the test.
SET @a=UNIX_TIMESTAMP("2010-01-21 15:32:22");
SET timestamp=@a;

--disable_warnings
SET DEBUG_SYNC= 'RESET';

# Create database
CREATE DATABASE IF NOT EXISTS ptr;
USE ptr;

--echo
--echo con1: ***Creating Tables***

CREATE TABLE ptr.t1(id INT, details CHAR(30))ENGINE=INNODB;
INSERT intO ptr.t1 VALUES
(1,'testing1'),(2,'testing2'),(3,'testing3'),
(4,'testing4'),(5,'testing5'),(6,'testing6');

CREATE TABLE ptr.t2(id INT, info CHAR(30))ENGINE=INNODB;
INSERT intO ptr.t2 VALUES(201, 'aa1'),(202,'aa2');

CREATE TABLE ptr.t3(id INT, name CHAR(20))ENGINE=INNODB;
INSERT intO ptr.t3 VALUES(1,'ab1'),(2,'ab2'),(3,'ab3'),(4,'ab4'),(5,'ab5');

--echo
--echo con2:
connection con2;
START TRANSACTION;
INSERT intO ptr.t2 VALUES(203, 'During backup-1'),(204,'During backup-2');
UPDATE ptr.t3 SET name='hh1' WHERE id=1;
DELETE FROM ptr.t1 WHERE id=5;
DELETE FROM ptr.t1 WHERE id=6;

--echo con1:
connection con1;
--echo The values should be: t1=6 t2=2 t3=5

SELECT * FROM ptr.t1;
SELECT COUNT(*) FROM ptr.t1;
SELECT * FROM ptr.t2;
SELECT COUNT(*) FROM ptr.t2;
SELECT * FROM ptr.t3;
SELECT COUNT(*) FROM ptr.t3;

# Activate the sync point for BACKUP.
# When BACKUP reaches the sync point it emits the signal bup_sync
# and waits for another thread to emit the signal bup_finish.
SET DEBUG_SYNC= 'before_backup_data_init SIGNAL bup_sync
                 WAIT_FOR bup_finish';
send BACKUP DATABASE ptr TO 'ptr_commit.bak';

--echo
--echo con3: Waiting for BACKUP to reach the breakpoint
connection con3;
# If BACKUP did already reach its sync point, then the signal is already
# present. If not, we wait until BACKUP emits it.
SET DEBUG_SYNC= 'now WAIT_FOR bup_sync';

--echo con2:
connection con2;
COMMIT;

--echo
--echo con3: Signalling that BACKUP can finish
connection con3;
SET DEBUG_SYNC= 'now SIGNAL bup_finish';

--echo con1:
connection con1;
--echo con1: Fetching result of BACKUP
--replace_column 1 #
reap;
set timestamp=@a+2;

--echo con2:
connection con2;
--echo After commit the values of will be: t1=4, t2=4, t3=5
SELECT COUNT(*) FROM ptr.t1;
SELECT * FROM ptr.t1;
SELECT COUNT(*) FROM ptr.t2;
SELECT * FROM ptr.t2;
SELECT COUNT(*) FROM ptr.t3;
SELECT * FROM ptr.t3;

LET $binlog_file =query_get_value(SHOW MASTER STATUS, File, 1);
LET $binlog_pos= query_get_value(SHOW MASTER STATUS, Position, 1);

--echo con1:
connection con1;
--echo **** Perform some operations after backup ****
INSERT intO ptr.t1 VALUES(10,'After Backup1'),(20,'After Backup2');
UPDATE ptr.t2 SET info='After Backup-1' WHERE id=202;
UPDATE ptr.t3 SET name='After Backup-1' WHERE id=2;
set timestamp=@a+4;
FLUSH LOGS;

--echo Values in table after backup: t1=6,t2=4(202 updated), t3=5(2 updated)
SELECT COUNT(*) FROM ptr.t1;
SELECT * FROM ptr.t1;
SELECT COUNT(*) FROM ptr.t2;
SELECT * FROM ptr.t2;
SELECT COUNT(*) FROM ptr.t3;
SELECT * FROM ptr.t3;

DROP DATABASE ptr;

--exec $MYSQL_BINLOG $MYSQLTEST_VARDIR/log/master-bin.000001 > $MYSQLTEST_VARDIR/tmp/qcommit.bin
--exec $MYSQL_BINLOG --stop-position=$binlog_pos $MYSQLTEST_VARDIR/log/$binlog_file |$MYSQL
--echo *** Recovering data till point of backup using mysql binlog_position ***

SELECT * FROM ptr.t1;
SELECT * FROM ptr.t2;
SELECT * FROM ptr.t3;

--echo *** Recover data after backup ***
--exec $MYSQL_BINLOG --start-position=$binlog_pos $MYSQLTEST_VARDIR/log/$binlog_file |$MYSQL

SELECT * FROM ptr.t1;
SELECT * FROM ptr.t2;
SELECT * FROM ptr.t3;

DROP DATABASE ptr;

--echo *** TEST2 ***
--echo Perform Restore and Recover committed data using mysqlbinlog position
--echo after backup.
--echo

--echo Perform restore operation
--replace_column 1 #
RESTORE FROM 'ptr_commit.bak';
SHOW TABLES FROM ptr;
SELECT * FROM ptr.t1;
SELECT * FROM ptr.t2;
SELECT * FROM ptr.t3;

--exec $MYSQL_BINLOG --start-position=$binlog_pos $MYSQLTEST_VARDIR/log/$binlog_file |$MYSQL
--echo *** Recover data using binlog position after backup ***

SELECT * FROM ptr.t1;
SELECT * FROM ptr.t2;
SELECT * FROM ptr.t3;
DROP DATABASE ptr;

--echo *** TEST3 ***
--echo Recover committed data during backup and after backup operation
--echo using mysqlbinlog dates
--echo 

--echo use binlog dates to recover data till backup.
--exec $MYSQL_BINLOG --start-datetime="2010-01-21 15:32:22" --stop-datetime="2010-01-21 15:32:24" $MYSQLTEST_VARDIR/log/master-bin.000001 |$MYSQL

SHOW TABLES FROM ptr;
SELECT * FROM ptr.t1;
SELECT * FROM ptr.t2;
SELECT * FROM ptr.t3;

--echo use binlog dates to recover data after backup.
--exec $MYSQL_BINLOG --start-datetime="2010-01-21 15:32:24" --stop-datetime="2010-01-21 15:32:26" $MYSQLTEST_VARDIR/log/master-bin.000001 |$MYSQL
--echo *** Now recover data after backup using binlog dates ***

SELECT * FROM ptr.t1;
SELECT * FROM ptr.t2;
SELECT * FROM ptr.t3;

--echo *** TEST4 *** 
--echo Perform Restore and Recover committed data using mysqlbinlog position
--echo after backup.

--echo Perform restore operation
--replace_column 1 #
RESTORE FROM 'ptr_commit.bak' OVERWRITE;
SHOW TABLES FROM ptr;
SELECT * FROM ptr.t1;
SELECT * FROM ptr.t2;
SELECT * FROM ptr.t3;

--exec $MYSQL_BINLOG --start-datetime="2010-01-21 15:32:24" --stop-datetime="2010-01-21 15:32:26" $MYSQLTEST_VARDIR/log/master-bin.000001 |$MYSQL
--echo *** Now recover data after backup using binlog dates ***

SELECT * FROM ptr.t1;
SELECT * FROM ptr.t2;
SELECT * FROM ptr.t3;

# Test cleanup section

--echo
--echo ***  DROP ptr DATABASE ****
--echo

DROP DATABASE ptr;
--remove_file $MYSQLTEST_VARDIR/master-data/ptr_commit.bak
SET DEBUG_SYNC= 'RESET';
