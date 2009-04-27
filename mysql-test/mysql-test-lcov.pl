#! /usr/bin/perl

# Copyright (C) 2008 Sun Microsystems, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

#
# Run lcov and genhtml to create a test coverage report.
#

use strict;
use warnings;
use Getopt::Long;

#
# Constants.
#
# Directory for results. Intermediate files and HTML result.
my $RESULT_DIR= "mysql-test/mysql-test-lcov";
#
# Files to ignore. E.g. files renamed after compilation.
my @EXCLUDE_FILES= qw{
storage/innobase/pars0grm
storage/innobase/lexyy
};

#
# Usage.
#
sub usage
{
  print <<"END";
Usage: $0 [options]

This program runs lcov for code coverage analysis, and genhtml to create
an HTML report in $RESULT_DIR/index.html.

Options:

  -h    --help        This help.
  -q    --quiet       Do not show commands run.
  -p    --purge       Delete all test coverage information, to prepare for a
                      new coverage test.

Prior to running this tool, MySQL should be compiled with
BUILD/compile-pentium-gcov, and the testsuite should be run.

Also you need to install the lcov package, which contains the
utilities lcov and genhtml.
END

  exit 1;
}

#
# Get command line options.
#
my $opt_quiet;
my $opt_help;
my $opt_purge;
my $result= GetOptions
  (
   "quiet"       => \$opt_quiet,
   "help"        => \$opt_help,
   "purge"       => \$opt_purge,
  );

usage() if $opt_help;

#
# Global variables.
#
my $cmd;
my $res;

#
# Hide EXCLUDE_FILES
#
sub hide_exclude_files()
{
  print STDERR "Hiding excluded files\n" if !$opt_quiet;
  foreach my $file ( @EXCLUDE_FILES )
  {
    unlink "$file.gcno.no-source";
    rename "$file.gcno", "$file.gcno.no-source";
    rename "$file.gcda", "$file.gcda.no-source";
  }
}

#
# Recover EXCLUDE_FILES
#
sub recover_exclude_files()
{
  print STDERR "Recovering excluded files\n" if !$opt_quiet;
  foreach my $file ( @EXCLUDE_FILES )
  {
    rename "$file.gcno.no-source", "$file.gcno";
    rename "$file.gcda.no-source", "$file.gcda";
  }
}

#
# In verbose mode we output to STDERR as well as to STDOUT.
# Avoid misplaced output due to buffering.
#
if (!$opt_quiet)
{
  select STDERR; $| = 1;      # make unbuffered
  select STDOUT; $| = 1;      # make unbuffered
}

#
# We need to change to the root directory of the source tree
# so that we find all sources, object files, and gcov files from ".".
#
my $troot= `bzr root`;
chomp $troot;
if (!$troot || !chdir $troot)
{
  die "Failed to find tree root. " .
      "(this tool must be run within a bzr work tree).\n";
}

#
# Purge counters from old test runs.
#
if($opt_purge)
{
  # One cannot create a file with empty name. But empty argument with -f
  # makes 'rm' silent when there is no file to remove.
  $cmd= "find . -name '*.da' -o -name '*.gcda' -o -name '*.gcov' |
         grep -v 'README\.gcov' |
         xargs rm -f ''";
  print STDERR "Running: $cmd\n" if !$opt_quiet;
  $res= system($cmd);
  exit($res ? ($? >> 8) : 0);
}

#
# Create result directory.
#
if (-d $RESULT_DIR)
{
  print STDERR "Recreating result directory: $RESULT_DIR\n" if !$opt_quiet;
  $res= system("rm -rf $RESULT_DIR");
  if ($res)
  {
    exit($res ? ($? >> 8) : 0);
  }
}
else
{
  print STDERR "Creating result directory: $RESULT_DIR\n" if !$opt_quiet;
}
$res= system("mkdir $RESULT_DIR");
if ($res)
{
  exit($res ? ($? >> 8) : 0);
}

#
# Hide EXCLUDE_FILES
#
hide_exclude_files();

#
# LCOV
#
$cmd= "lcov -q -c -f -d . -o $RESULT_DIR/lcov.info";
print STDERR "Running: $cmd\n" if !$opt_quiet;
$res= system($cmd);
if ($res)
{
  recover_exclude_files();
  exit($res ? ($? >> 8) : 0);
}

#
# GENHTML
#
$cmd= "genhtml -q --prefix=`pwd` -o $RESULT_DIR " .
               "$RESULT_DIR/lcov.info";
print STDERR "Running: $cmd\n" if !$opt_quiet;
$res= system($cmd);
if ($res)
{
  recover_exclude_files();
  exit($res ? ($? >> 8) : 0);
}

#
# Recover EXCLUDE_FILES
#
recover_exclude_files();

print "Result is in $RESULT_DIR/index.html\n";

