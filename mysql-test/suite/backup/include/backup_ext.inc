
# The external program "mysql_convert_table_format" is not incorporated with
# mtr framework. Therefore, in order to use this program in a portable way, 
# we have created the following script.
# The following script will find path to mysql_convert_table_format utility
# in a portable way and without modifying mtr.pl

--perl
use strict;
use lib "lib/";

use IO::File;
use File::Basename;
use My::Find;

my $basedir= dirname($ENV{MYSQL_TEST_DIR}) or die "Need \$MYSQL_TEST_DIR";
my $vardir= $ENV{MYSQLTEST_VARDIR} or die  "Need \$MYSQLTEST_VARDIR";

my $location= my_find_bin($basedir,
			["scripts"],
		        "mysql_convert_table_format",
			NOT_REQUIRED);

# In the actual test, source $MYSQLTEST_VARDIR/tmp/mctf.inc and this will get
# $MYSQL_CONVERT_TABLE_FORMAT set to point to the correct location and ready to # be used.

my $F = IO::File->new("$vardir/tmp/mctf.inc", "w") or die;
print $F "let \$MYSQL_CONVERT_TABLE_FORMAT = $location;";

EOF
