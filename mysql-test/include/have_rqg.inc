--disable_query_log

# Check that we have RQG setup

if (`select '$RQG_HOME'=''`)
{
--skip Could not locate RQG - set RQG_HOME variable.
}

# setup $RQG variable

let $RQG= perl $RQG_HOME/gensql.pl --seed=0;

--enable_query_log
