##########################################################################
# Author: Hema
# Date: 2008-07-30
# Purpose: Test to ensure that point in time recovery preserves all the      
# objects during backup. We include objects like tables, views, triggers,
# stored procedures and stored functions. This test also verifies the
# dependencies in objects are recovered properly using mysql binlog position
# binlog dates.
###############################################################################

--source include/not_embedded.inc
--source include/have_log_bin.inc
SELECT @@BINLOG_FORMAT;
RESET MASTER;

--error 0,1
--remove_file $MYSQLTEST_VARDIR/master-data/ptr_objects.bak;

--echo **START TEST**
--echo *** TEST1 ***
--echo This test will recover data and objects using binlog position
--echo till point of backup and after backup.
--echo

#We set constant timestamp inside the test.
SET @a=UNIX_TIMESTAMP("2010-01-21 15:32:22");
SET timestamp=@a;

#
# Two databases are created ptr_ob1 and ptr_ob2.
# ptr_ob1 has tables t1, t2, t3, tv1 and views v ,v1, vv.
# v --> ptr_ob1.t1, v1 --> ptr_ob2.t1, vv --> ptr_ob2.v1
# ptr_ob2 has tables t1, t2 and views v1.
# v1 --> ptr_ob1.tv1 + ptr_ob2.t2
# trg1(before insert), trg2(after insert), trg4(after delete) and 
# trg5(before update) are in ptr_ob1.
# trg3(before delete) and trg6(after update) are in ptr_ob2.
# procedure p1, p2 and p3 are created in ptr_ob1 and p4 in ptr_ob2.
#

# Create database
CREATE DATABASE IF NOT EXISTS ptr_ob1;
CREATE DATABASE IF NOT EXISTS ptr_ob2;
USE ptr_ob1;

--echo **** Creating tables ****

CREATE TABLE ptr_ob1.t1(
 id INT, 
 a INT, 
 b CHAR(5)
);

CREATE TABLE ptr_ob1.t2(
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, 
  data CHAR(50)
);

CREATE TABLE ptr_ob1.t3(
 srno INT AUTO_INCREMENT, 
 PRIMARY KEY(srno), 
 name VARCHAR(30) NOT NULL, 
 old_a INT
); 

CREATE TABLE ptr_ob2.t1(
 id INT, 
 b CHAR(5)
);

CREATE TABLE ptr_ob2.t2(id SMALLINT);

INSERT INTO ptr_ob2.t1 VALUES(11,'set1'),(21,'set2'),(3,'set3'),(4,'set4');
INSERT INTO ptr_ob2.t2 VALUES(91),(5),(26),(33),(5);

--echo **** Creating views ****

CREATE VIEW ptr_ob1.v AS SELECT * FROM ptr_ob1.t1;
CREATE VIEW ptr_ob1.v1 AS SELECT * FROM ptr_ob2.t1;

CREATE TABLE ptr_ob1.tv1(id SMALLINT);
INSERT INTO ptr_ob1.tv1  VALUES(1),(2),(3),(3),(5);
SELECT * FROM ptr_ob1.tv1 UNION ALL SELECT * FROM ptr_ob2.t2;
CREATE VIEW ptr_ob2.v1 AS SELECT * FROM ptr_ob1.tv1  UNION ALL 
SELECT * FROM ptr_ob2.t2 ORDER BY id;
SELECT * FROM ptr_ob2.v1;

--echo #### Creating view from another view ####
CREATE VIEW ptr_ob1.vv AS SELECT SUM(id) from ptr_ob2.v1 GROUP BY id;
SELECT * FROM ptr_ob1.vv;

--echo **** Creating Triggers ****

delimiter ||;
CREATE TRIGGER ptr_ob1.trg1 BEFORE INSERT ON ptr_ob1.t1 FOR EACH ROW
BEGIN
 INSERT INTO ptr_ob1.t3 VALUES(NULL, 't1', new.a);
END;||
delimiter ;||

INSERT INTO ptr_ob1.t1 VALUES(1,20, 'bb1'), (2,50, 'bb2'),(3,80,'bb3');
SELECT * FROM ptr_ob1.t1;
SELECT * FROM ptr_ob1.t3;

delimiter ||;
CREATE TRIGGER ptr_ob1.trg2 AFTER INSERT ON ptr_ob1.t2 FOR EACH ROW
BEGIN
 INSERT INTO ptr_ob1.t3 VALUES(NULL, 'trigger fired for AFTER INSERT', 100);
END;||
delimiter ;||

INSERT INTO ptr_ob1.t2 VALUES
(NULL, 'Normal Insert1'),(NULL, 'Normal Insert2'),
(NULL, 'Normal Insert3'),(NULL, 'Normal Insert4');
SELECT * FROM ptr_ob1.t2;
SELECT * FROM ptr_ob1.t3;

delimiter ||;
CREATE TRIGGER ptr_ob2.trg3 BEFORE DELETE ON ptr_ob2.t2 FOR EACH ROW
 SET @del_sum:= @del_sum + old.id;||
 SET @del_sum:= 0;||
delimiter ;||

DELETE FROM ptr_ob2.t2 WHERE id=26;
DELETE FROM ptr_ob2.t2 WHERE id=91;
SELECT @del_sum;

delimiter ||;
CREATE TRIGGER ptr_ob1.trg4 AFTER DELETE ON ptr_ob1.t1 FOR EACH ROW
BEGIN 
 INSERT INTO ptr_ob2.t1(id, b) VALUES(old.id, old.b);
END;||
delimiter ;||

DELETE FROM ptr_ob1.t1 WHERE id=2;
SELECT * FROM ptr_ob1.t1;
SELECT * FROM ptr_ob2.t1;

delimiter ||;
CREATE TRIGGER ptr_ob1.trg5 BEFORE UPDATE ON ptr_ob1.t1 FOR EACH ROW
BEGIN
 SET @id=old.id;
 SET @a_old=old.a;
 SET @a_new=new.a;
END;
||
delimiter ;||

UPDATE ptr_ob1.t1 SET a='85' WHERE b='bb3';
SELECT @id, @a_old, @a_new;

delimiter ||;
CREATE TRIGGER ptr_ob2.trg6 AFTER UPDATE ON ptr_ob2.t1 FOR EACH ROW
BEGIN
 INSERT INTO ptr_ob1.t2 VALUES
 (NULL,'Trigger fired from after update of ptr_ob2.t1');  
END;
||
delimiter ;||

UPDATE ptr_ob2.t1 SET b='alert' WHERE id=3;
SELECT * FROM ptr_ob2.t1;
SELECT * FROM ptr_ob1.t2;
SELECT * FROM ptr_ob1.v1;

--echo
--echo **** Creating stored Procedures and functions ****
--echo

--echo #### The procedure p1 will insert VALUES in view ####
delimiter ||;
CREATE PROCEDURE ptr_ob1.p1(details CHAR(40), id INT)
BEGIN
  INSERT INTO ptr_ob1.v1 VALUES(1,'fired');
END;
||

--echo #### Procedure p2 will trigger trg2 ####

CREATE PROCEDURE ptr_ob1.p2(content VARCHAR(40))
BEGIN
 INSERT INTO ptr_ob1.t2 VALUES(NULL, 'Trigger fired from procedure p2');
END;||

--echo #### Procedure p3 will trigger trg6 ####

CREATE PROCEDURE ptr_ob1.p3(info TEXT)
BEGIN
 UPDATE ptr_ob2.t1 SET b='#up#' WHERE id=4;
END;||

--echo #### Procedure p4 will call view ####

CREATE PROCEDURE ptr_ob2.p4() 
BEGIN
 SELECT * FROM ptr_ob2.v1;
END;
||

--echo #### Function f1() ####

CREATE FUNCTION f1() RETURNS INT
RETURN (SELECT COUNT(*) FROM ptr_ob1.v1);||
DELIMITER ;||

SELECT f1();

CALL ptr_ob1.p1('procedure p1 called for view v1', 1);
CALL ptr_ob1.p1('procedure p1 called for view v1', 2);
SELECT * FROM ptr_ob1.v1;
SELECT * FROM ptr_ob2.t1;

CALL ptr_ob1.p2('procedure p2 called for trg2');
CALL ptr_ob1.p2('procedure p2 again called for trg2');

SELECT * FROM ptr_ob1.t2;
SELECT * FROM ptr_ob1.t3;

CALL ptr_ob1.p3('procedure p3 called for trg6');

SELECT * FROM ptr_ob2.t1;
SELECT * FROM ptr_ob1.t2;

CALL ptr_ob2.p4();

SELECT * FROM ptr_ob2.v1;
SELECT * FROM ptr_ob1.vv;
SELECT * FROM ptr_ob1.tv1;

--echo ** Checking data contents in all the tables before performing backup **

--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW PROCEDURE STATUS;
--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW TRIGGERS FROM ptr_ob1;
--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW TRIGGERS FROM ptr_ob2;
SHOW FULL TABLES FROM ptr_ob1;
SHOW FULL TABLES FROM ptr_ob2;
SELECT * FROM ptr_ob1.t1;
SELECT * FROM ptr_ob1.v;
SELECT * FROM ptr_ob1.t2;
SELECT * FROM ptr_ob1.t3;
SELECT * FROM ptr_ob2.t1;
SELECT * FROM ptr_ob1.v1;
SELECT * FROM ptr_ob1.tv1;
SELECT * FROM ptr_ob2.v1;
SELECT * FROM ptr_ob1.vv;
SELECT ptr_ob1.f1();

# Perform  backup operation and obtain binlog position and dates.
LET $backup_id = `BACKUP DATABASE ptr_ob1, ptr_ob2 TO 'ptr_objects.bak'`;
set timestamp=@a+2;
LET $binlog_pos = `SELECT binlog_pos FROM mysql.backup_history WHERE backup_id =$backup_id`;
LET $binlog_file =query_get_value(SHOW MASTER STATUS, File, 1);

--echo Make some changes to objects.

ALTER VIEW ptr_ob1.v AS SELECT id, a FROM ptr_ob1.t1;
ALTER TABLE ptr_ob1.vv RENAME TO ptr_ob1.vva;
ALTER TABLE ptr_ob2.t2 ADD COLUMN af_backup VARCHAR(30);
ALTER TABLE ptr_ob1.t3 CHANGE name name TEXT;
DESCRIBE ptr_ob1.t3;
ALTER PROCEDURE ptr_ob2.p4 COMMENT 'AFTER BACKUP';
SELECT * FROM ptr_ob1.v;

--echo Creating some new objects

CREATE TABLE ptr_ob1.t11 (i INT, sr INT, j INT) engine=myisam;
INSERT INTO ptr_ob1.t11 VALUES (1, 2, 3);
CREATE TRIGGER ptr_ob1.ai AFTER INSERT ON ptr_ob1.t11 FOR EACH ROW SET @a:= new.sr;
CREATE TRIGGER ptr_ob1.au AFTER UPDATE ON ptr_ob1.t11 FOR EACH ROW SET @a:= new.sr;
CREATE TRIGGER ptr_ob1.ad AFTER DELETE ON ptr_ob1.t11 FOR EACH ROW SET @a:= old.sr;

INSERT INTO ptr_ob1.t11 VALUES(4, 5, 6),(7, 8, 9);
SELECT @a;
UPDATE ptr_ob1.t11 SET i=10 WHERE sr=5;
SELECT @a;
DELETE FROM ptr_ob1.t11 WHERE sr=8;
SELECT @a;

--echo Now alter the table 

ALTER TABLE ptr_ob1.t11 DROP COLUMN j;
SELECT * FROM ptr_ob1.t11;
INSERT INTO ptr_ob1.t11 VALUES(11, 12);
SELECT @a;

--echo Firing existing triggers and calling procedures.

INSERT INTO ptr_ob1.t2 VALUES(NULL, 'Insert after backup');
INSERT INTO ptr_ob2.t2 VALUES(10, 'af_bk1'),(2, 'af_bk2'),(7,'af_bk3'),(2,'af_bk4');
DELETE FROM ptr_ob2.t2 WHERE id=10;
SELECT @del_sum;

CALL ptr_ob1.p1('procedure p1 af_bkup for view v1', 3);
SELECT * FROM ptr_ob1.v1;
SELECT * FROM ptr_ob2.t1;

CALL ptr_ob1.p2('procedure p2 called  af_bkup for trg2');
SELECT * FROM ptr_ob1.t2;
SELECT * FROM ptr_ob1.t3;

CALL ptr_ob1.p3('procedure p3 called af_bkup for trg6');
SELECT * FROM ptr_ob2.t1;
SELECT * FROM ptr_ob1.t2;

CALL ptr_ob2.p4();
SELECT * FROM ptr_ob2.v1;
SELECT * FROM ptr_ob1.vva;
SELECT * FROM ptr_ob1.tv1;

# Changes made after backup are:
# ptr_ob1.v is altered, ptr_ob1.vv is renamed as vva.
# ptr_ob2.t2 has new column added, ptr_ob1.t3 has datatype change.
# Table ptr_ob1.t11 is created and triggers ptr_ob1.ai, au, ad is created.
# procedure ptr_ob2.p4 is altered.

--echo Checking the data contents after backup.

--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW PROCEDURE STATUS;
--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW TRIGGERS FROM ptr_ob1;
--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW TRIGGERS FROM ptr_ob2;
SHOW FULL TABLES FROM ptr_ob1;
SHOW FULL TABLES FROM ptr_ob2;
SELECT * FROM ptr_ob1.t1;
SELECT * FROM ptr_ob1.v;
SELECT * FROM ptr_ob1.t2;
SELECT * FROM ptr_ob1.t3;
SELECT * FROM ptr_ob2.t1;
SELECT * FROM ptr_ob1.v1;
SELECT * FROM ptr_ob1.tv1;
SELECT * FROM ptr_ob2.v1;
SELECT * FROM ptr_ob1.vva;
SELECT * FROM ptr_ob1.t11;
SELECT ptr_ob1.f1();

SET TIMESTAMP=@a+4;
LET $binpos=query_get_value(SHOW MASTER STATUS, Position, 1);
LET $binfile=query_get_value(SHOW MASTER STATUS, File, 1);

# Rotate logs. This frees the previous log so that we can dump it.
FLUSH LOGS;
DROP DATABASE ptr_ob1;
DROP DATABASE ptr_ob2;

--echo **** Execute mysqlbinlog to recover the data from start to backup ****

--exec $MYSQL_BINLOG  $MYSQLTEST_VARDIR/log/master-bin.000001 > $MYSQLTEST_VARDIR/tmp/qobject.bin
--exec $MYSQL_BINLOG  --stop-position=$binlog_pos $MYSQLTEST_VARDIR/log/$binlog_file |$MYSQL

# Verify that the table we just dropped has been restored

--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW PROCEDURE STATUS;
--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW TRIGGERS FROM ptr_ob1;
--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW TRIGGERS FROM ptr_ob2;
SHOW FULL TABLES FROM ptr_ob1;
SHOW FULL TABLES FROM ptr_ob2;
SELECT * FROM ptr_ob1.t1;
SELECT * FROM ptr_ob1.v;
SELECT * FROM ptr_ob1.t2;
SELECT * FROM ptr_ob1.t3;
SELECT * FROM ptr_ob2.t1;
SELECT * FROM ptr_ob1.v1;
SELECT * FROM ptr_ob1.tv1;
SELECT * FROM ptr_ob2.v1;
SELECT * FROM ptr_ob1.vv;
SELECT ptr_ob1.f1();

--echo **** Now perform another recovery to recover data after backup ****

--exec $MYSQL_BINLOG --start-position=$binlog_pos $MYSQLTEST_VARDIR/log/$binlog_file |$MYSQL

--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW PROCEDURE STATUS;
--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW TRIGGERS FROM ptr_ob1;
--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW TRIGGERS FROM ptr_ob2;
SHOW FULL TABLES FROM ptr_ob1;
SHOW FULL TABLES FROM ptr_ob2;
SELECT * FROM ptr_ob1.t1;
SELECT * FROM ptr_ob1.v;
SELECT * FROM ptr_ob1.t2;
SELECT * FROM ptr_ob1.t3;
SELECT * FROM ptr_ob2.t1;
SELECT * FROM ptr_ob1.v1;
SELECT * FROM ptr_ob1.tv1;
SELECT * FROM ptr_ob2.v1;
SELECT * FROM ptr_ob1.vva;
SELECT * FROM ptr_ob1.t11;
SELECT ptr_ob1.f1();

DROP DATABASE ptr_ob1;
DROP DATABASE ptr_ob2;

--echo *** TEST2 *** :
--echo Perform Restore and recover transactions after backup by executing 
--echo mysqlbinlog utility using binlog position.
--echo

--echo Perfrom Restore operation
--replace_column 1 #
RESTORE FROM 'ptr_objects.bak';

--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW PROCEDURE STATUS;
--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW TRIGGERS FROM ptr_ob1;
--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW TRIGGERS FROM ptr_ob2;
SHOW FULL TABLES FROM ptr_ob1;
SHOW FULL TABLES FROM ptr_ob2;
SELECT * FROM ptr_ob1.t1;
SELECT * FROM ptr_ob1.v;
SELECT * FROM ptr_ob1.t2;
SELECT * FROM ptr_ob1.t3;
SELECT * FROM ptr_ob2.t1;
SELECT * FROM ptr_ob1.v1;
SELECT * FROM ptr_ob1.tv1;
SELECT * FROM ptr_ob2.v1;
SELECT * FROM ptr_ob1.vv;
SELECT ptr_ob1.f1();

--echo Now use binlog position to recover data after backup
--exec $MYSQL_BINLOG --start-position=$binlog_pos $MYSQLTEST_VARDIR/log/$binlog_file |$MYSQL

--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW PROCEDURE STATUS;
--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW TRIGGERS FROM ptr_ob1;
--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW TRIGGERS FROM ptr_ob2;
SHOW FULL TABLES FROM ptr_ob1;
SHOW FULL TABLES FROM ptr_ob2;
SELECT * FROM ptr_ob1.t1;
SELECT * FROM ptr_ob1.v;
SELECT * FROM ptr_ob1.t2;
SELECT * FROM ptr_ob1.t3;
SELECT * FROM ptr_ob2.t1;
SELECT * FROM ptr_ob1.v1;
SELECT * FROM ptr_ob1.tv1;
SELECT * FROM ptr_ob2.v1;
SELECT * FROM ptr_ob1.vva;
SELECT * FROM ptr_ob1.t11;
SELECT ptr_ob1.f1();

DROP DATABASE ptr_ob1;
DROP DATABASE ptr_ob2;

--echo *** TEST3 *** :
--echo This test will recover data and objects using binlog dates
--echo till point of backup and after backup.
--echo

--echo Use mysqlbinlog dates to recover data till point of backup.
--exec $MYSQL_BINLOG --start-datetime="2010-01-21 15:32:22" --stop-datetime="2010-01-21 15:32:24" $MYSQLTEST_VARDIR/log/master-bin.000001 |$MYSQL

--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW PROCEDURE STATUS;
--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW TRIGGERS FROM ptr_ob1;
--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW TRIGGERS FROM ptr_ob2;
SHOW FULL TABLES FROM ptr_ob1;
SHOW FULL TABLES FROM ptr_ob2;
SELECT * FROM ptr_ob1.t1;
SELECT * FROM ptr_ob1.v;
SELECT * FROM ptr_ob1.t2;
SELECT * FROM ptr_ob1.t3;
SELECT * FROM ptr_ob2.t1;
SELECT * FROM ptr_ob1.v1;
SELECT * FROM ptr_ob1.tv1;
SELECT * FROM ptr_ob2.v1;
SELECT * FROM ptr_ob1.vv;
SELECT ptr_ob1.f1();

--echo Use mysqlbinlog dates to recover data after backup 
--exec $MYSQL_BINLOG --start-datetime="2010-01-21 15:32:24" --stop-datetime="2010-01-21 15:32:26" $MYSQLTEST_VARDIR/log/master-bin.000001 |$MYSQL

--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW PROCEDURE STATUS;
--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW TRIGGERS FROM ptr_ob1;
--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW TRIGGERS FROM ptr_ob2;
SHOW FULL TABLES FROM ptr_ob1;
SHOW FULL TABLES FROM ptr_ob2;
SELECT * FROM ptr_ob1.t1;
SELECT * FROM ptr_ob1.v;
SELECT * FROM ptr_ob1.t2;
SELECT * FROM ptr_ob1.t3;
SELECT * FROM ptr_ob2.t1;
SELECT * FROM ptr_ob1.v1;
SELECT * FROM ptr_ob1.tv1;
SELECT * FROM ptr_ob2.v1;
SELECT * FROM ptr_ob1.vva;
SELECT * FROM ptr_ob1.t11;
SELECT ptr_ob1.f1();

DROP DATABASE ptr_ob1;
DROP DATABASE ptr_ob2;

--echo *** TEST4 *** :
--echo Perform Restore and recover objects after backup by executing mysqlbinlog
--echo utility using binlog dates.
--echo

--echo Perform Restore operation
--replace_column 1 #
RESTORE FROM 'ptr_objects.bak';

--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW PROCEDURE STATUS;
--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW TRIGGERS FROM ptr_ob1;
--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW TRIGGERS FROM ptr_ob2;
SHOW FULL TABLES FROM ptr_ob1;
SHOW FULL TABLES FROM ptr_ob2;
SELECT * FROM ptr_ob1.t1;
SELECT * FROM ptr_ob1.v;
SELECT * FROM ptr_ob1.t2;
SELECT * FROM ptr_ob1.t3;
SELECT * FROM ptr_ob2.t1;
SELECT * FROM ptr_ob1.v1;
SELECT * FROM ptr_ob1.tv1;
SELECT * FROM ptr_ob2.v1;
SELECT * FROM ptr_ob1.vv;
SELECT ptr_ob1.f1();

--echo use binlog dates to recover data after backup.
--exec $MYSQL_BINLOG --start-datetime="2010-01-21 15:32:24" --stop-datetime="2010-01-21 15:32:26" $MYSQLTEST_VARDIR/log/master-bin.000001 |$MYSQL

--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW PROCEDURE STATUS;
--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW TRIGGERS FROM ptr_ob1;
--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW TRIGGERS FROM ptr_ob2;
SHOW FULL TABLES FROM ptr_ob1;
SHOW FULL TABLES FROM ptr_ob2;
SELECT * FROM ptr_ob1.t1;
SELECT * FROM ptr_ob1.v;
SELECT * FROM ptr_ob1.t2;
SELECT * FROM ptr_ob1.t3;
SELECT * FROM ptr_ob2.t1;
SELECT * FROM ptr_ob1.v1;
SELECT * FROM ptr_ob1.tv1;
SELECT * FROM ptr_ob2.v1;
SELECT * FROM ptr_ob1.vva;
SELECT * FROM ptr_ob1.t11;
SELECT ptr_ob1.f1();

DROP DATABASE ptr_ob1;
DROP DATABASE ptr_ob2;

--echo *** TEST5 *** :
--echo Do complete recovery of data after backup using binlog position. Ensure
--echo that presence of backup operation in the binlog shouldn't have any impact

--exec $MYSQL_BINLOG  --stop-position=$binpos $MYSQLTEST_VARDIR/log/$binfile |$MYSQL

--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW PROCEDURE STATUS;
--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW TRIGGERS FROM ptr_ob1;
--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW TRIGGERS FROM ptr_ob2;
SHOW FULL TABLES FROM ptr_ob1;
SHOW FULL TABLES FROM ptr_ob2;
SELECT * FROM ptr_ob1.t1;
SELECT * FROM ptr_ob1.v;
SELECT * FROM ptr_ob1.t2;
SELECT * FROM ptr_ob1.t3;
SELECT * FROM ptr_ob2.t1;
SELECT * FROM ptr_ob1.v1;
SELECT * FROM ptr_ob1.tv1;
SELECT * FROM ptr_ob2.v1;
SELECT * FROM ptr_ob1.vva;
SELECT * FROM ptr_ob1.t11;
SELECT ptr_ob1.f1();

DROP DATABASE ptr_ob1;
DROP DATABASE ptr_ob2;

--echo *** TEST6 *** :
--echo Do complete recovery of data after backup using binlog dates. Ensure
--echo that presence of backup operation in the binlog shouldn't have any impact

--exec $MYSQL_BINLOG --start-datetime="2010-01-21 15:32:22" --stop-datetime="2010-01-21 15:32:26" $MYSQLTEST_VARDIR/log/master-bin.000001 |$MYSQL

--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW PROCEDURE STATUS;
--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW TRIGGERS FROM ptr_ob1;
--replace_column 5 # 6 # 9 # 10 # 11 #
SHOW TRIGGERS FROM ptr_ob2;
SHOW FULL TABLES FROM ptr_ob1;
SHOW FULL TABLES FROM ptr_ob2;
SELECT * FROM ptr_ob1.t1;
SELECT * FROM ptr_ob1.v;
SELECT * FROM ptr_ob1.t2;
SELECT * FROM ptr_ob1.t3;
SELECT * FROM ptr_ob2.t1;
SELECT * FROM ptr_ob1.v1;
SELECT * FROM ptr_ob1.tv1;
SELECT * FROM ptr_ob2.v1;
SELECT * FROM ptr_ob1.vva;
SELECT * FROM ptr_ob1.t11;
SELECT ptr_ob1.f1();

# Test cleanup section

--echo
--echo ***  DROP ptr_ob1 and ptr_ob2 DATABASE ****
--echo

DROP DATABASE ptr_ob1;
DROP DATABASE ptr_ob2;
--remove_file $MYSQLTEST_VARDIR/master-data/ptr_objects.bak



