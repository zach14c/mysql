#
# This include file is created to test the missing use dependencies. The main
# test will refer to this file for testing use dependencies scenarios.
#

--echo Perform restore again to get all objects back.
--replace_column 1 #
RESTORE FROM 'ob.bak' OVERWRITE;

eval DROP $drop_object;

--disable_warnings
--replace_column 1 #
eval BACKUP DATABASE $db TO 'ob_missing.bak';
--enable_warnings

--replace_column 2 #
SHOW WARNINGS; 

--disable_warnings
--replace_column 1 #
eval RESTORE FROM 'ob_missing.bak' OVERWRITE;
--enable_warnings

--replace_column 2 #
SHOW WARNINGS; 

#verify the status of all objects:
SHOW DATABASES LIKE 'ob%';
CALL test.show_objects('ob1');
CALL test.show_objects('ob2');
CALL test.show_objects('ob3');

#clean-up section
--remove_file $bdir/ob_missing.bak

