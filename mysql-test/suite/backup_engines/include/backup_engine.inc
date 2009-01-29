###############################################################################
# Author: Hema
# Date: 2008-04-01
# Purpose:To define the engine variable.
###############################################################################
--replace_column 2 #
SHOW VARIABLES LIKE 'storage_engine';
let $ENGINE= query_get_value(SHOW VARIABLES LIKE 'storage_engine', Value, 1);

