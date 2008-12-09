# Created 2008-12-04 HHunger
# Check the System variable "thread_handling".
--disable_query_log
# Check if thread_handling= one-thread-per-connection
let $no_needed_threads= `SELECT @@GLOBAL.thread_handling != 'one-thread-per-connection'`;
if ($no_needed_threads)
{
  skip Test needs thread_handling=one-thread-per-connection;
}
--enable_query_log

