##
## Checks for the presence of the Falcon storage engine (plugin)
## by querying the INFORMATION_SCHEMA.ENGINES table.
##
## If Falcon is not available, the test using this include file
## is skipped.
##

# Do not echo these statements to test results
--disable_query_log

# Check if Falcon is available
let $have_falcon = `SELECT (support = 'YES' OR support = 'DEFAULT' OR support = 'ENABLED') AS 'HAVE_FALCON' FROM INFORMATION_SCHEMA.ENGINES WHERE ENGINE = 'Falcon'`;
if (!$have_falcon)
{
  skip Test needs the Falcon storage engine;
}
--enable_query_log
