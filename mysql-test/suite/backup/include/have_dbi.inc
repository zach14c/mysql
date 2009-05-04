# This script is written in order to skip the test if perl DBI modules are not
# installed in the system

disable_query_log;
let $suffix= `SELECT UUID()`;
enable_query_log;

let $dbi_exists_file= $MYSQLTEST_VARDIR/tmp/dbi_exists.$suffix;
let TMP_FILE= $dbi_exists_file;

perl;
my $success= 1;
my $file= $ENV{'TMP_FILE'};
eval "require DBI";
eval "require DBD::mysql";
if ($@)
{
  $success= 0;
}
open FH, "> $file";
print FH "let \$dbi_exists= $success;";
close FH; 
EOF

source $dbi_exists_file;
remove_file $dbi_exists_file;
if(!$dbi_exists) { 
  skip DBI & DBD modules not installed.;  
}
