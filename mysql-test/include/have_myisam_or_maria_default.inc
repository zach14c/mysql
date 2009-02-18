# This is for tests which create a table using the default engine
# and expect MyISAM-like behaviour
disable_query_log;
let $have_myisam_or_maria_default=`select (@@storage_engine in ("myisam","maria")) as `TRUE``;
if (!$have_myisam_or_maria_default)
{
  skip Test relies on MyISAM or Maria being the default storage engine;
}
enable_query_log;
