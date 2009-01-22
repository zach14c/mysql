#
# SUMMARY
#
#   Locks a table in one thread, run an SQL statement in another thread,
#   and check that the SQL statement is locked.
#
# USAGE
#
#   let $lock_table= t1;          # The table to be locked
#   let $sql= DROP TABLE t1 ;     # The SQL statement that should lock
#
# OPTIONAL
#   let $sync= after_start_ddl;      # What to sync with to unlock the tables.
#                                    # Default: wait_for_lock
#   let $setup= HANDLER t1 OPEN;     # Statement to run before the locked
#                                    # statement
#   let $teardown= HANDLER t1 CLOSE; # Statement to run after the lock has
#                                    # been unlocked
#
# EXAMPLE
#   concurrent_ddl.test in ddl_lock suite.

--disable_warnings
SET DEBUG_SYNC= 'RESET';
--enable_warnings

# Default value
if (`SELECT LENGTH("$sync") = 0`) {
  let $sync= mdl_enter_cond;
}

--echo # Connection waiter will run setup (if any)
connection waiter;
if (`SELECT LENGTH("$setup")`) {
  eval $setup;
}

--echo # Using sync_lock.inc to synchronize on '$sync'

--echo # Connection locker will lock the resource
connection locker;

SET DEBUG_SYNC= 'after_lock_tables_takes_lock SIGNAL locked WAIT_FOR do_unlock';
send;
eval LOCK TABLES $lock_table READ;

--echo # Connection waiter will wait for the resource to be locked, and then
--echo # run the SQL that should lock.
connection waiter;

SET DEBUG_SYNC= 'now WAIT_FOR locked';
# run mysql-test-run --debug for information about the signal
--replace_result $sync <sync>
eval SET DEBUG_SYNC= '$sync SIGNAL do_unlock';
send;
eval $sql;

--echo # Connection locker will unlock the locked resources
connection locker;
UNLOCK TABLES;

--echo # Connection waiter will wait for the SQL to return after the
--echo # lock is unlocked, and run teardown if we have any
connection waiter;
reap;
if (`SELECT LENGTH("$teardown")`) {
  eval $teardown;
}

--echo # Cleaning up in default connection
connection default;

--disable_warnings
SET DEBUG_SYNC= 'RESET';
--enable_warnings
