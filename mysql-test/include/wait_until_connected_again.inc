#
# Include this script to wait until the connection to the
# server has been restored or timeout occurs.
#
# You should have done --enable_reconnect first.
#
# When you change this file you may have to chance its cousin
# wait_until_disconnected.inc

--disable_result_log
--disable_query_log
# We wait for 7 minutes, because some storage engines need quite
# some to restart with Valgrind runs on Pushbuild.
let $counter= 4200;
let $mysql_errno= 9999;
while ($mysql_errno)
{
  # Strangely enough, the server might return "Too many connections"
  # while being shutdown, thus 1040 is an "allowed" error
  # See BUG#36228
  --error 0,1040,1053,2002,2003,2006,2013
  show status;

  dec $counter;
  if (!$counter)
  {
    --die Server failed to restart
  }
  --real_sleep 0.1
}
--enable_query_log
--enable_result_log
