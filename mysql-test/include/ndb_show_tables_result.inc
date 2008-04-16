--disable_warnings
--disable_query_log
CREATE TEMPORARY TABLE IF NOT EXISTS ndb_show_tables_results (id INT, type VARCHAR(20), state VARCHAR(20), logging VARCHAR(20), _database VARCHAR(255), _schema VARCHAR(20), name VARCHAR(255));

let $dump_file= $MYSQLTEST_VARDIR/tmp/ndb_show_tables.dat;
--exec $NDB_TOOLS_DIR/ndb_show_tables --p > $dump_file

eval LOAD DATA INFILE '$dump_file' INTO TABLE ndb_show_tables_results;

remove_file $dump_file;

--enable_query_log
--enable_warnings
--echo
--echo ndb_show_tables completed.....
--echo
