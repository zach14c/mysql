disable_query_log;
let $have_blackhole=`select (support = 'YES' or support = 'DEFAULT') as `TRUE` 
                   from information_schema.engines where engine = 'blackhole'`;
if (!$have_blackhole)
{
  skip Test needs the Blackhole storage engine;
}
enable_query_log;
