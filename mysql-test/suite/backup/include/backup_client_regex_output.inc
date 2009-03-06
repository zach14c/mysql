#
# Note: Use of regular expressions in this file:
#
#   For cases where program output is expected.
#
#      It consists of the following replacements:
#
#      /Image path: [^[.newline.]]*/Image path: #/
#        Replace everything behind "Image path: " until newline,
#        but excluding newline by "#".
#
#      /Image size: [^[.newline.]]*/Image size: #/
#        Replace everything behind "Image size: " until newline,
#        but excluding newline by "#".
#
#      / time: [^[.newline.]]*/ time: #/
#        Replace everything behind " time: " until newline,
#        but excluding newline by "#".
#
#      /Server version: [^[.newline.]]*/Server version: #/
#        Replace everything behind "Server version: " until newline,
#        but excluding newline by "#".
#
#      /Server byte order: [^[.newline.]]*/Server byte order: #/
#        Replace everything behind "Server byte order: " until newline,
#        but excluding newline by "#".
#
#      /Image options: [^[.newline.]]*/Image options: #/
#        Replace everything behind "Image options: " until newline,
#        but excluding newline by "#". Due to endian differences.
#
#      /'([[:alpha:]]:)?[\/\\][^']*'/#/
#        Replace everything between single quotes, that starts with
#        an optional drive letter and colon plus a slash or backslash
#        by "#". The single quotes are also replaced.
#
#      /.*mysqlbackup(\.exe)?: Out of memory [^[.newline.]]*/mysqlbackup: Out of memory/
#        Replace everything before "mysqlbackup" or "mysqlbackup.exe",
#        if followed by ": Out of memory ", and everything behind
#        "Out of memory" until newline, but excluding newline by nothing.
#        This settles the differences in output between
#        my_malloc.c:my_malloc() and safemalloc.c:_mymalloc().
#
#      /.*mysqlbackup(\.exe)?: needed .*//
#        Replace everything before "mysqlbackup" or "mysqlbackup.exe",
#        if followed by ": needed ", and everything behind
#        and everything behind "needed" until newline,
#        and this time *in*cluding newline by nothing.
#        This settles the differences in output between
#        my_malloc.c:my_malloc() and safemalloc.c:_mymalloc().
#        The latter produces one extra line, which is removed here.
#
#      /STARTS '....-..-.. ..:..:..'/STARTS 'yyyy-mm-dd HH:MM:SS'/
#        Replaces event start time by fixed pattern.
#
#      /non-existent (.*) position: [0-9]*/non-existent \1 position: #/
#        Replace catalog position numbers by '#'.
#        This appears as error messages in coverage tests.
#
#      /Binlog (coordinates|group coords): [^[.newline.]]*/Binlog \1: #/
#        Replace binlog coordinates by '#'.
#
# Note: --replace_regex does neither allow continuation lines,
#                       nor variable substitution (e.g. replace_regex $EXPR).
#
--replace_regex /Image path: [^[.newline.]]*/Image path: #/ /Image size: [^[.newline.]]*/Image size: #/ / time: [^[.newline.]]*/ time: #/ /Server version: [^[.newline.]]*/Server version: #/ /Server byte order: [^[.newline.]]*/Server byte order: #/ /Image options: [^[.newline.]]*/Image options: #/ /'([[:alpha:]]:)?[\/\\][^']*'/#/ /.*mysqlbackup(\.exe)?: Out of memory [^[.newline.]]*/mysqlbackup: Out of memory/ /.*mysqlbackup(\.exe)?: needed .*// /STARTS '....-..-.. ..:..:..'/STARTS 'yyyy-mm-dd HH:MM:SS'/ /non-existent (.*) position: [0-9]*/non-existent \1 position: #/ /Binlog (coordinates|group coords): [^[.newline.]]*/Binlog \1: #/
