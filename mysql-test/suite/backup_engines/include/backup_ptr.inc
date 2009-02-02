##########################################################################
# Author: Hema
# Date: 2008-07-30
# Purpose: Test to ensure that point in time recovery is preserved during 
# backup. In this test we perform both Data Manipulation Operations and 
# Data Definition Operations. We recover the data after and before backup
# using both binlog positions and binlog dates.
###############################################################################


--source include/not_embedded.inc
--source include/have_log_bin.inc
SELECT @@BINLOG_FORMAT;
RESET MASTER;

--error 0,1
--remove_file $MYSQLTEST_VARDIR/master-data/ptr.bak; 

--echo **START TEST**
--echo **** TEST1 ****
--echo  This test will recover data using binlog position
--echo  till point of backup and after backup.
--echo

#We set constant timestamp inside the test.
SET @a=UNIX_TIMESTAMP("2010-01-21 15:32:22");
SET timestamp=@a;

# Create database
CREATE DATABASE IF NOT EXISTS ptr;
USE ptr;

--echo **** Creating tables ****
CREATE TABLE ptr.t1(id INT, a CHAR(4));
INSERT INTO ptr.t1 VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');
SELECT * FROM ptr.t1;

CREATE TABLE ptr.t3(id INT, b CHAR(4));
INSERT INTO ptr.t3 VALUES
(11,'aa'),(22,'bb'),(33,'cc'),(44,'dd'),(55,'ee');

CREATE TABLE ptr.t2(id INT, deletes_data VARCHAR(30), updates_data TEXT);
INSERT INTO ptr.t2 VALUES
(100,'data1 for testing','update data1'),
(200,'data2 for testing','update data2'),
(300,'data3 for testing','update data3'),
(400,'data4 for testing','update data4'),
(500,'data5 for testing','update data5'),
(100,'data1 for testing','update data1'),
(600,'data6 for testing','update data6');
SELECT * FROM ptr.t2 ORDER BY id;
SELECT COUNT(*) FROM ptr.t2;

--echo **** Creating tables with different datatypes ****

CREATE TABLE ptr.d1(
rint INT,
tint TINYINT,
sint SMALLINT,
bint BIGINT,
mint MEDIUMINT,
name CHAR(100),
city  VARCHAR(100),
fl FLOAT(7,4),
pers DECIMAL(8,2),
sal DOUBLE,
colours SET('red','blue','yellow'),
continent ENUM('Asia', 'Europe','Africa','Antartica'),
ts TIMESTAMP DEFAULT 0,
dob DATE,
y YEAR
);

CREATE TABLE ptr.d2(
region TEXT,
summary LONGTEXT,
data BLOB,
details MEDIUMBLOB,
queries TINYTEXT,
query2 TINYBLOB,
extract LONGBLOB,
paras MEDIUMTEXT
);

INSERT INTO ptr.d1 VALUES
(785,127,7288,278829899,3777,'testing1','sweden','678.299',200.23,829899.909,
'yellow','Asia','2008-06-01 16:23:30','1984-09-08','1984');

INSERT INTO ptr.d2 VALUES
('xxxxxxxx','Testofonline backup','aaaaaaaaaa','bbbbbbbbbbb','hhhhhhhhhhh',
'kkkkkkkkkkkkk','mmmmmmmmmmmm','onlinebackup1');

SELECT * FROM ptr.d1;
SELECT * FROM ptr.d2;

--echo **** Perform Data Manipulation Operations ****
--echo ## Delete ##
DELETE FROM ptr.t2 WHERE deletes_data='data4 for testing';
DELETE FROM ptr.t2 WHERE id=100 LIMIT 1;

--echo ## Insert and Select ##
--echo **** Now ptr.t2 will have 5 counts in the table ****
--echo 
SELECT COUNT(*) FROM ptr.t2;
INSERT INTO ptr.t2 VALUES
(800,'data1 for replacement','replace data1');

--echo ## Replace ##
REPLACE INTO ptr.t2 
SET id=800, deletes_data='data replaced', updates_data='replace over';

--echo ## Union ##
SELECT * FROM ptr.t1 UNION SELECT * FROM ptr.t3;

--echo ## Update ##
UPDATE ptr.t2 SET updates_data='##VALUE UPDATED##' WHERE id=300;

--echo  Now table t2 will have 6 counts with one updated and replaced values 
SELECT COUNT(*) FROM ptr.t2;
SELECT * FROM ptr.t2 ORDER BY id;

DESCRIBE ptr.t1;
DESCRIBE ptr.t2;
DESCRIBE ptr.d1;
DESCRIBE ptr.d2;

#--echo ## Perform table maintenence operations ##
#We disable the table maintenence operations as it is supported by only
#Myisam and Innodb engines. Falcon engine does not support table maintenance
#operations.
 
#ANALYZE TABLE d1;
#ANALYZE TABLE t2;
#CHECK TABLE d1;
#CHECK TABLE t2;

--echo **** Perform Data Definition Operations ****

--echo ## ALTER ##
ALTER TABLE ptr.t2 ADD COLUMN changes CHAR(20);

UPDATE ptr.t2 SET changes='altered' WHERE id=200;
UPDATE ptr.t2 SET changes='altered' WHERE id=100;

ALTER TABLE ptr.t3 MODIFY b VARCHAR(4);

DESCRIBE ptr.t2;
DESCRIBE ptr.t3;

--echo **** Now we will check the data contents in all the tables again ****

SELECT * FROM ptr.t1;
SELECT * FROM ptr.t2 ORDER BY id;
SELECT * FROM ptr.t3;
SELECT * FROM ptr.d1;
SELECT * FROM ptr.d2;

--echo ** Perform Backup operation and gather binlog position and dates **
--echo ** Backup data **
LET $backup_id = `BACKUP DATABASE ptr TO 'ptr.bak'`;
set timestamp=@a+2;
LET $binlog_pos = `SELECT binlog_pos FROM mysql.backup_history WHERE backup_id =$backup_id`;
LET $binlog_file =query_get_value(SHOW MASTER STATUS, File, 1);

--echo
--echo Now execute some operations in the table after performing backup.
--echo

--echo
--echo **** Perform some data manipulation operations ****

INSERT INTO ptr.t1 VALUES(6,'f'),(7,'g');
INSERT INTO ptr.d1 VALUES
(10001,120,6550,7278634657,90667,'After backup','Minneapolis',782.9901,
789.23,97806.456,'blue','Antartica','2008-08-07 15:12:44','1954-12-23','1954');

INSERT INTO ptr.t3 VALUES(1,'ff'),(3,'kk');
SELECT * FROM t1 UNION SELECT * FROM t3 ORDER BY a;

UPDATE ptr.t3 SET b='mm' where b='kk';
SELECT * FROM ptr.t3;

DELETE FROM ptr.t2 WHERE id=200;
SELECT * FROM ptr.t2 ORDER BY id;

--echo
--echo **** Perform some data definition operations ****

ALTER TABLE ptr.t2 DROP COLUMN changes;
ALTER TABLE ptr.t3 ENGINE=MEMORY;
CREATE TABLE definition(details LONGTEXT);
INSERT INTO definition VALUES
('Performing some data definition statements for PTR');

DESCRIBE ptr.t2;
DESCRIBE ptr.definition;

--echo
--echo **** The tables t1, t2, t3 and t4 has changes ****
--echo **** New table "definition" is created after backup *****
--echo

SHOW TABLES FROM ptr;
SELECT * FROM ptr.t1;
SELECT * FROM ptr.t2 ORDER BY id;
SELECT * FROM ptr.t3;
SELECT * FROM ptr.d1;
SELECT * FROM ptr.d2;

SET TIMESTAMP=@a+4;
LET $binpos=query_get_value(SHOW MASTER STATUS, Position, 1);
LET $binfile=query_get_value(SHOW MASTER STATUS, File, 1);

# Rotate logs. This frees the previous log so that we can dump it.
FLUSH LOGS;
DROP DATABASE ptr;

--echo **** Execute mysqlbinlog to recover the data from start to backup ****

--exec $MYSQL_BINLOG --database=ptr $MYSQLTEST_VARDIR/log/master-bin.000001 > $MYSQLTEST_VARDIR/tmp/q1.bin
--exec $MYSQL_BINLOG --database=ptr --stop-position=$binlog_pos $MYSQLTEST_VARDIR/log/$binlog_file |$MYSQL

# Verify that the table we just dropped has been restored
SHOW TABLES FROM ptr;
SELECT * FROM ptr.t1;
SELECT * FROM ptr.t2 ORDER BY id;
SELECT * FROM ptr.t3;
SELECT * FROM ptr.d1;
SELECT * FROM ptr.d2;

--echo **** Now perform another recovery to recover data after backup ****
--exec $MYSQL_BINLOG --database=ptr --start-position=$binlog_pos $MYSQLTEST_VARDIR/log/$binlog_file |$MYSQL

SHOW TABLES FROM ptr;

DESCRIBE ptr.t1;
DESCRIBE ptr.t2;
DESCRIBE ptr.t3;

SELECT * FROM ptr.t1;
SELECT * FROM ptr.t2 ORDER BY id;
SELECT * FROM ptr.t3;
SELECT * FROM ptr.d1;
SELECT * FROM ptr.d2;

DROP DATABASE ptr;

--echo *** TEST2 *** :
--echo Perform Restore and recover transactions after backup by executing 
--echo mysqlbinlog utility using binlog position.

--echo Perform restore operation 
--replace_column 1 #
RESTORE FROM 'ptr.bak';
SELECT * FROM ptr.t1;
SELECT * FROM ptr.t2 ORDER BY id;
SELECT * FROM ptr.t3;
SELECT * FROM ptr.d1;
SELECT * FROM ptr.d2;

--echo Recovering data contents after backup
--exec $MYSQL_BINLOG --database=ptr --start-position=$binlog_pos  $MYSQLTEST_VARDIR/log/$binlog_file |$MYSQL
SHOW TABLES FROM ptr;

DESCRIBE ptr.t1;
DESCRIBE ptr.t2;
DESCRIBE ptr.t3;

SELECT * FROM ptr.t1;
SELECT * FROM ptr.t2 ORDER BY id;
SELECT * FROM ptr.t3;
SELECT * FROM ptr.d1;
SELECT * FROM ptr.d2;

DROP DATABASE ptr;

--echo *** TEST3 *** :
--echo This test will recover data using binlog dates
--echo till point of backup and after backup.

--echo  Now use binlog dates to recover the data till point of backup.

--exec $MYSQL_BINLOG --database=ptr --start-datetime="2010-01-21 15:32:22" --stop-datetime="2010-01-21 15:32:24" $MYSQLTEST_VARDIR/log/master-bin.000001 |$MYSQL

SELECT * FROM ptr.t1;
SELECT * FROM ptr.t2 ORDER BY id;
SELECT * FROM ptr.t3;
SELECT * FROM ptr.d1;
SELECT * FROM ptr.d2;

--echo We execute binlog to recover data after backup using binlog dates.

--exec $MYSQL_BINLOG --database=ptr --start-datetime="2010-01-21 15:32:24" --stop-datetime="2010-01-21 15:32:26" $MYSQLTEST_VARDIR/log/master-bin.000001 |$MYSQL
SHOW TABLES FROM ptr;

DESCRIBE ptr.t1;
DESCRIBE ptr.t2;
DESCRIBE ptr.t3;

SELECT * FROM ptr.t1;
SELECT * FROM ptr.t2 ORDER BY id;
SELECT * FROM ptr.t3;
SELECT * FROM ptr.d1;
SELECT * FROM ptr.d2;
DROP DATABASE ptr;

--echo *** TEST4 *** :
--echo Perform Restore and recover transactions after backup by executing
--echo mysqlbinlog utility using binlog dates.

--replace_column 1 #
RESTORE FROM 'ptr.bak';

DESCRIBE ptr.t1;
DESCRIBE ptr.t2;
DESCRIBE ptr.t3;

SELECT * FROM ptr.t1;
SELECT * FROM ptr.t2 ORDER BY id;
SELECT * FROM ptr.t3;
SELECT * FROM ptr.d1;
SELECT * FROM ptr.d2;

--exec $MYSQL_BINLOG --database=ptr --start-datetime="2010-01-21 15:32:24" --stop-datetime="2010-01-21 15:32:26" $MYSQLTEST_VARDIR/log/master-bin.000001 |$MYSQL

SHOW DATABASES;
SHOW TABLES FROM ptr;

DESCRIBE ptr.t1;
DESCRIBE ptr.t2;
DESCRIBE ptr.t3;

SELECT * FROM ptr.t1;
SELECT * FROM ptr.t2 ORDER BY id;
SELECT * FROM ptr.t3;
SELECT * FROM ptr.d1;
SELECT * FROM ptr.d2;
DROP DATABASE ptr;

--echo *** TEST5 *** :
--echo Do complete recovery of data after backup using binlog position. Ensure 
--echo that presence of backup operation in the binlog shouldn't have any impact

--exec $MYSQL_BINLOG --database=ptr  --stop-position=$binpos $MYSQLTEST_VARDIR/log/$binfile |$MYSQL

SHOW DATABASES;
SHOW TABLES FROM ptr;

DESCRIBE ptr.t1;
DESCRIBE ptr.t2;
DESCRIBE ptr.t3;

SELECT * FROM ptr.t1;
SELECT * FROM ptr.t2 ORDER BY id;
SELECT * FROM ptr.t3;
SELECT * FROM ptr.d1;
SELECT * FROM ptr.d2;
DROP DATABASE ptr;

--echo *** TEST6 *** :
--echo Do complete recovery of data after backup using binlog dates. Ensure
--echo that presence of backup operation in the binlog shouldn't have any impact

--exec $MYSQL_BINLOG --database=ptr --start-datetime="2010-01-21 15:32:22" --stop-datetime="2010-01-21 15:32:26" $MYSQLTEST_VARDIR/log/master-bin.000001 |$MYSQL

SHOW DATABASES;
SHOW TABLES FROM ptr;

DESCRIBE ptr.t1;
DESCRIBE ptr.t2;
DESCRIBE ptr.t3;

SELECT * FROM ptr.t1;
SELECT * FROM ptr.t2 ORDER BY id;
SELECT * FROM ptr.t3;
SELECT * FROM ptr.d1;
SELECT * FROM ptr.d2;

# Test cleanup section

--echo
--echo ***  DROP ptr DATABASE ****
--echo

DROP DATABASE ptr;
--remove_file $MYSQLTEST_VARDIR/master-data/ptr.bak
