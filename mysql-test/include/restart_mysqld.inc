#
# A macro to restart a certain server.
# The caller can call `let $server_id_for_restart = N' prior
# invocation for the macro to change the default server id

let $server_id_for_restart = 1;

#
# Write file to make mysql-test-run.pl expect the "crash", but don't start
# it until it's told to.
# The name of the expect file must correspond to one that mtr expects
# which is a name of the section for the server in the server's configuration
# file.
#

--write_file $MYSQLTEST_VARDIR/tmp/mysqld.$server_id_for_restart.expect
wait
EOF

# Send shutdown to the connected server and give
# it 10 seconds to die before zapping it
shutdown_server 10;

# Check server is gone
--source include/wait_until_disconnected.inc

# Write file to make mysql-test-run.pl start up the server again
--append_file $MYSQLTEST_VARDIR/tmp/mysqld.$server_id_for_restart.expect
restart
EOF

# Turn on reconnect
--enable_reconnect

# Call script that will poll the server waiting for it to be back online again
--source include/wait_until_connected_again.inc

# Turn off reconnect again
--disable_reconnect

