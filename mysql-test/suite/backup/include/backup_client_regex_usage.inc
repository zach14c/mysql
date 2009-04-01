#
# Note: Use of regular expressions in this file:
#
#   For cases where mysqlbackup client program usage is printed.
#
#      It consists of the following replacements:
#
#      /.*mysqlbackup(\.exe)? Ver .* for .* at [^[.newline.]]*/mysqlbackup Ver # for # at #/
#        Replace everything before "mysqlbackup" or "mysqlbackup.exe",
#        if followed by version and machine information by nothing, and
#        everything between the fixed words and everything behind "at"
#        until newline, but excluding newline by "#".
#
#      /Usage: .*mysqlbackup(\.exe)? /Usage: mysqlbackup /
#        Replace everything between "Usage: " and "mysqlbackup" or
#        "mysqlbackup.exe" by nothing.
#
#      /.*mysqlbackup(\.exe)?: unknown option /mysqlbackup: unknown option /
#        Replace everything before "mysqlbackup" or "mysqlbackup.exe",
#        if followed by ": unknown option" by nothing.
#
#      /.*debug.*//
#        Remove all lines that contain 'debug'. '.*' at end includes newline.
#        Debug binary shows debug option and variable in the help message.
#
#      /open_files_limit   [^[.newline.]]*/open_files_limit #/
#        Replace everything behind "open_files_limit   " until newline,
#        but excluding newline by "#".
#
# Note: --replace_regex does neither allow continuation lines,
#                       nor variable substitution (e.g. replace_regex $EXPR).
#
--replace_regex /.*mysqlbackup(\.exe)? Ver .* for .* at [^[.newline.]]*/mysqlbackup Ver # for # at #/ /Usage: .*mysqlbackup(\.exe)? /Usage: mysqlbackup / /.*mysqlbackup(\.exe)?: unknown option /mysqlbackup: unknown option / /.*debug.*// /open_files_limit   [^[.newline.]]*/open_files_limit #/
