/* Copyright (c) 2008 Sun Microsystems, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

   NOTE: When changing this copyright notice,
         check also the one in the usage() function.
*/

/*
  MySQL Backup Utility
*/

/* Include client related stuff, which includes a lot of common stuff. */
#include "client_priv.h"

/* As long as we don't connect to the server, we are not a MYSQL_CLIENT. */
#undef MYSQL_CLIENT

/* my_init_time() */
#include "my_time.h"

/* MYSQL_SERVER_VERSION */
#include "mysql_version.h"

/* Include from the stream access functions. */
#include "backup_stream.h"

/* isspace() */
#include <ctype.h>

/*
  Configuration file.
*/
static const char *load_default_groups[]= { "mysqlbackup",
#ifdef MYSQL_CLIENT
                                            "client",
#endif
                                            NULL };

/*
  Command line options.
*/

/* General command line options. */
static uint             opt_verbose;
static ulong            opt_open_files_limit;
#ifndef DBUG_OFF
static const char *default_dbug_option = "d:t:o,/tmp/mysqlbackup.trace";
#endif

#ifdef MYSQL_CLIENT
/* Command line options required by a MySQL client (connecting to a server). */
static const char       *opt_database;
static const char       *opt_host;
static char             *opt_pass;
static int              opt_port;
static uint             opt_protocol;
static const char       *opt_sock;
static const char       *opt_user;
#endif

/* mysqlbackup specific command line options. */
static bool             opt_mysqlbackup_catalog_summary;
static bool             opt_mysqlbackup_catalog_details;
static bool             opt_mysqlbackup_metadata_statements;
static bool             opt_mysqlbackup_metadata_extra;
static bool             opt_mysqlbackup_snapshots;
static bool             opt_mysqlbackup_data_chunks;
static bool             opt_mysqlbackup_data_totals;
static bool             opt_mysqlbackup_summary;
static bool             opt_mysqlbackup_exact;
static bool             opt_mysqlbackup_image_order;
static const char       *opt_mysqlbackup_search;
/*
  Option numbers start above those from enum options_client (client_priv.h).
  The enum itself is not used, just its values.
*/
enum options_mysqlbackup
{
  OPT_MYSQLBACKUP_CATALOG_SUMMARY= OPT_MAX_CLIENT_OPTION,
  OPT_MYSQLBACKUP_CATALOG_DETAILS,
  OPT_MYSQLBACKUP_METADATA_STATEMENTS,
  OPT_MYSQLBACKUP_METADATA_EXTRA,
  OPT_MYSQLBACKUP_SNAPSHOTS,
  OPT_MYSQLBACKUP_DATA_CHUNKS,
  OPT_MYSQLBACKUP_DATA_TOTALS,
  OPT_MYSQLBACKUP_SUMMARY,
  OPT_MYSQLBACKUP_ALL,
  OPT_MYSQLBACKUP_EXACT,
  OPT_MYSQLBACKUP_IMAGE_ORDER,
  OPT_MYSQLBACKUP_SEARCH,

  /* End of options terminator. */
  OPT_MYSQLBACKUP_LAST
};

/* Array of command line options. */
static struct my_option my_long_options[] =
{
  /* General options. */
  {"help", '?', "Display this help and exit.",
   0, 0, 0, GET_NO_ARG, NO_ARG, 0, 0, 0, 0, 0, 0},
#ifdef __NETWARE__
  {"autoclose", OPT_AUTO_CLOSE, "Auto close the screen on exit for Netware.",
   0, 0, 0, GET_NO_ARG, NO_ARG, 0, 0, 0, 0, 0, 0},
#endif
#ifndef DBUG_OFF
  {"debug", '#', "Output debug log.", (uchar**) &default_dbug_option,
   (uchar**) &default_dbug_option, 0, GET_STR, OPT_ARG, 0, 0, 0, 0, 0, 0},
#endif
  {"verbose", 'v', "Print verbose information.",
   0, 0, 0, GET_NO_ARG, NO_ARG, 0, 0, 0, 0, 0, 0},
  {"version", 'V', "Print version and exit.", 0, 0, 0, GET_NO_ARG, NO_ARG, 0,
   0, 0, 0, 0, 0},
  {"open_files_limit", OPT_OPEN_FILES_LIMIT,
   "Used to reserve file descriptors for usage by this program.",
   (uchar**) &opt_open_files_limit, (uchar**) &opt_open_files_limit,
   0, GET_ULONG, REQUIRED_ARG, MY_NFILE, 8, OS_FILE_LIMIT, 0, 1, 0},

#ifdef MYSQL_CLIENT
  /* Options required by a MySQL client (connecting to a server). */
  {"database", 'd', "Unused.",
   (uchar**) &opt_database, (uchar**) &opt_database,
   0, GET_STR_ALLOC, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
  {"host", 'h', "Unused.",
   (uchar**) &opt_host, (uchar**) &opt_host,
   0, GET_STR_ALLOC, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
  {"password", 'p', "Unused.",
   0, 0, 0, GET_STR, OPT_ARG, 0, 0, 0, 0, 0, 0},
  {"port", 'P', "Unused.",
   (uchar**) &opt_port, (uchar**) &opt_port, 0, GET_INT, REQUIRED_ARG,
   0, 0, 0, 0, 0, 0},
  {"protocol", OPT_MYSQL_PROTOCOL, "Unused.",
   0, 0, 0, GET_STR,  REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
  {"socket", 'S', "Unused.",
   (uchar**) &opt_sock, (uchar**) &opt_sock,
   0, GET_STR, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
  {"user", 'u', "Unused.",
   (uchar**) &opt_user, (uchar**) &opt_user,
   0, GET_STR_ALLOC, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
#endif /*MYSQL_CLIENT*/

  /* mysqlbackup specific options. */
  {"catalog-summary", OPT_MYSQLBACKUP_CATALOG_SUMMARY,
   "Print summary from the database objects catalog.",
   0, 0, 0, GET_NO_ARG, NO_ARG, 0, 0, 0, 0, 0, 0},

  {"catalog-details", OPT_MYSQLBACKUP_CATALOG_DETAILS,
   "Print details from the database objects catalog.",
   0, 0, 0, GET_NO_ARG, NO_ARG, 0, 0, 0, 0, 0, 0},

  {"metadata-statements", OPT_MYSQLBACKUP_METADATA_STATEMENTS,
   "Print SQL statements to create the database objects.",
   0, 0, 0, GET_NO_ARG, NO_ARG, 0, 0, 0, 0, 0, 0},

  {"metadata-extra", OPT_MYSQLBACKUP_METADATA_EXTRA,
   "Print extra meta data for the database objects.",
   0, 0, 0, GET_NO_ARG, NO_ARG, 0, 0, 0, 0, 0, 0},

  {"snapshots", OPT_MYSQLBACKUP_SNAPSHOTS,
   "Print information about snapshots contained in the backup image.",
   0, 0, 0, GET_NO_ARG, NO_ARG, 0, 0, 0, 0, 0, 0},

  {"data-chunks", OPT_MYSQLBACKUP_DATA_CHUNKS,
   "Print length of every data chunk contained in the backup image.",
   0, 0, 0, GET_NO_ARG, NO_ARG, 0, 0, 0, 0, 0, 0},

  {"data-totals", OPT_MYSQLBACKUP_DATA_TOTALS,
   "Print length of data contained in the backup image for each object.",
   0, 0, 0, GET_NO_ARG, NO_ARG, 0, 0, 0, 0, 0, 0},

  {"summary", OPT_MYSQLBACKUP_SUMMARY,
   "Print summary information from end of the backup image.",
   0, 0, 0, GET_NO_ARG, NO_ARG, 0, 0, 0, 0, 0, 0},

  {"all", OPT_MYSQLBACKUP_ALL,
   "Print everything except snapshots and data-chunks.",
   0, 0, 0, GET_NO_ARG, NO_ARG, 0, 0, 0, 0, 0, 0},

  {"exact", OPT_MYSQLBACKUP_EXACT,
   "Print exact number of bytes instead of human readable form.",
   0, 0, 0, GET_NO_ARG, NO_ARG, 0, 0, 0, 0, 0, 0},

  {"image-order", OPT_MYSQLBACKUP_IMAGE_ORDER,
   "Print catalog items and meta data in the order of the backup image.",
   0, 0, 0, GET_NO_ARG, NO_ARG, 0, 0, 0, 0, 0, 0},

  {"search", OPT_MYSQLBACKUP_SEARCH,
   "Search object in the backup image. "
   "Name can be object or database.object. "
   "Quoting of database and/or object with \", ', or ` is allowed. "
   "Wildcards % and _ are available. "
   "Use with --metadata-* options to see meta data. "
   "Plain name finds global objects, name1.name2 finds per db objects.",
   (uchar**) &opt_mysqlbackup_search, (uchar**) &opt_mysqlbackup_search,
   0, GET_STR, REQUIRED_ARG, 0, 0, 0, 0, 0, 0},

  /* End of options terminator. */
  {0, 0, 0, 0, 0, 0, GET_NO_ARG, NO_ARG, 0, 0, 0, 0, 0, 0}
};

/* Values, derived from command line options. */
static char             **defaults_argv;
static char             *search_database_name;
static char             *search_object_name;

/* Flags, determining, which backup image sections we need to read. */
static bool             need_catalog;
static bool             need_metadata;
static bool             need_tabledata;
static bool             need_summary;

/* Flags for print_item_*(). */
#define PRI_NAME        0x0001
#define PRI_META        0x0002
#define PRI_SPACE       0x0004


#include <help_start.h>

/**
  Print client version.
*/

static void
print_version(void)
{
  printf("%s Ver %s for %s at %s compiled %s\n",
         my_progname, MYSQL_SERVER_VERSION,
         SYSTEM_TYPE, MACHINE_TYPE, __DATE__);
  NETWARE_SET_SCREEN_MODE(1);
}


/**
  Print client command line usage.
*/

static void
usage(void)
{
  print_version();

  /*
    NOTE: When changing this copyright notice,
          check also the one at top of this file.
  */
  puts("\nCopyright (c) 2008 Sun Microsystems, Inc.\n"
       "This program is free software; you can redistribute it and/or modify\n"
       "it under the terms of the GNU General Public License as published by\n"
       "the Free Software Foundation; version 2 of the License.\n");

  puts("This program displays information from a backup image.\n");

  printf("Usage: %s [options] backup-image-file\n", my_progname);

  my_print_help(my_long_options);
  my_print_variables(my_long_options);
}

#include <help_end.h>


/**
  Treat some options specially.

  Called for each option that appears on the command line.

  @param[in]    optid           numeric option identifier, often a char value
  @param[in]    opt             pointer to option initializer
  @param[in]    argument        option argument, can be NULL

  @return       status
    @retval     FALSE           ok
    @retval     TRUE            error
*/

extern "C" my_bool
get_one_option(int optid, const struct my_option *opt __attribute__((unused)),
               char *argument)
{
#ifdef MYSQL_CLIENT
  bool tty_password=0;
#endif

  switch (optid) {
  /* General options. */
#ifdef __NETWARE__
  case OPT_AUTO_CLOSE:
    setscreenmode(SCR_AUTOCLOSE_ON_EXIT);
    break;
#endif
#ifndef DBUG_OFF
  case '#':
    DBUG_PUSH(argument ? argument : default_dbug_option);
    break;
#endif
  case 'v':
    opt_verbose++;
    break;
  case 'V':
    print_version();
    exit(0);
  case '?':
    usage();
    exit(0);

#ifdef MYSQL_CLIENT
  /* Options required by a MySQL client (connecting to a server). */
  case 'p':
    if (argument)
    {
      my_free(opt_pass,MYF(MY_ALLOW_ZERO_PTR));
      char *start=argument;
      opt_pass= my_strdup(argument,MYF(MY_FAE));
      while (*argument) *argument++= 'x';       /* Destroy argument */
      if (*start)
        start[1]=0;                             /* Cut length of argument */
    }
    else
      tty_password=1;
    break;
  case OPT_MYSQL_PROTOCOL:
    opt_protocol= find_type_or_exit(argument, &sql_protocol_typelib,
                                    opt->name);
    break;
#endif /*MYSQL_CLIENT*/

  /* mysqlbackup specific options. */
  case OPT_MYSQLBACKUP_CATALOG_SUMMARY:
    opt_mysqlbackup_catalog_summary= TRUE;
    need_catalog= TRUE;
    break;
  case OPT_MYSQLBACKUP_CATALOG_DETAILS:
    opt_mysqlbackup_catalog_details= TRUE;
    need_catalog= TRUE;
    break;
  case OPT_MYSQLBACKUP_METADATA_STATEMENTS:
    opt_mysqlbackup_metadata_statements= TRUE;
    need_metadata= TRUE;
    break;
  case OPT_MYSQLBACKUP_METADATA_EXTRA:
    opt_mysqlbackup_metadata_extra= TRUE;
    need_metadata= TRUE;
    break;
  case OPT_MYSQLBACKUP_SNAPSHOTS:
    opt_mysqlbackup_snapshots= TRUE;
    /* We display the table names with each snapshot. */
    need_catalog= TRUE;
    break;
  case OPT_MYSQLBACKUP_DATA_CHUNKS:
    opt_mysqlbackup_data_chunks= TRUE;
    need_tabledata= TRUE;
    break;
  case OPT_MYSQLBACKUP_DATA_TOTALS:
    opt_mysqlbackup_data_totals= TRUE;
    need_tabledata= TRUE;
    break;
  case OPT_MYSQLBACKUP_SUMMARY:
    opt_mysqlbackup_summary= TRUE;
    need_summary= TRUE;
    break;
  case OPT_MYSQLBACKUP_ALL:
    opt_mysqlbackup_catalog_summary= TRUE;
    opt_mysqlbackup_catalog_details= TRUE;
    opt_mysqlbackup_metadata_statements= TRUE;
    opt_mysqlbackup_metadata_extra= TRUE;
    opt_mysqlbackup_data_totals= TRUE;
    opt_mysqlbackup_summary= TRUE;
    need_catalog= TRUE;
    need_metadata= TRUE;
    need_tabledata= TRUE;
    need_summary= TRUE;
    break;
  case OPT_MYSQLBACKUP_EXACT:
    opt_mysqlbackup_exact= TRUE;
    break;
  case OPT_MYSQLBACKUP_IMAGE_ORDER:
    opt_mysqlbackup_image_order= TRUE;
    break;
  case OPT_MYSQLBACKUP_SEARCH:
    /* We need the catalog for searching. */
    need_catalog= TRUE;
    break;
  }

#ifdef MYSQL_CLIENT
  if (tty_password)
    opt_pass= get_tty_password(NullS);
#endif /*MYSQL_CLIENT*/

  return 0;
}


/**
  Print a message to stderr.

  The format string should include newlines ('\n') where required
  to end a print line.

  @param[in]    format          printf-style format string
  @param[in]    ...             printf-style varargs
*/

extern "C" void
errm(const char *format, ...) ATTRIBUTE_FORMAT(printf, 1, 2);
extern "C" void
errm(const char *format, ...)
{
  va_list args;
  DBUG_ASSERT(format);

  /*
    Before printing the error message, flush all prior output.
    stdout may be buffered depending on platform and output device.
  */
  fflush(stdout);

  /* Write prefix. */
  fprintf(stderr, "\nERROR: ");

  /* Write error message. */
  va_start(args, format);
  vfprintf(stderr, format, args);
  va_end(args);

  /* Append a newline to make it stand out. */
  fprintf(stderr, "\n");

  /*
    Flush the error message, so that it appears as soon as possible.
    stderr may be buffered depending on platform and output device.
  */
  fflush(stderr);
}


/**
  Skip space.

  @param[in]    ptr             current string position

  @return       pointer to non-space character
*/

static const char*
skip_space(const char *ptr)
{
  DBUG_ASSERT(ptr);

  while (isspace((int)(unsigned char) *ptr))
    ptr++;

  return ptr;
}


/**
  Find token end.

  @param[in]    ptr             current string position
  @param[in]    terminator      character that terminates token
  @param[in]    quoted          if token is quoted

  @return       pointer to first character not belonging to token
*/

static const char*
token_end(const char *ptr, const char terminator, bool quoted)
{
  DBUG_ASSERT(ptr);

  if (quoted)
  {
    /*
      If quoted, terminator is the non-NUL quote character.
      In a quoted token, space does not terminate the token.
    */
    while (*ptr && (*ptr != terminator))
      ptr++;
  }
  else if (terminator == '\0')
  {
    /* In an un-quoted token, space terminates the token. */
    while (*ptr && !isspace((int)(unsigned char) *ptr))
      ptr++;
  }
  else
  {
    /* In an un-quoted token, space terminates the token. */
    while (*ptr && (*ptr != terminator) && !isspace((int)(unsigned char) *ptr))
      ptr++;
  }

  return ptr;
}


/**
  Extract object name.

  The object name is either a sequence of non-space characters or quoted.

  If the first non-space character is a quote character, the object name
  is terminated by a matching quote character only. The quote characters
  do not belong to the object name. The trailing quote character is skipped.

  If the first non-space character is not a quote character, the object
  name can also be terminated by a terminator character. The terminator
  character is not skipped.

  The function skips leading and trailing space.

  @param[in]    ptr             current string position
  @param[in]    terminator      character that terminates object name
  @param[out]   object_p        allocated object name

  @return       pointer to next non-space character or string end
*/

static const char*
extract_object_name(const char *ptr, const char terminator, char **object_p)
{
  const char    *token;
  const char    *tend;
  char          quote;
  DBUG_ASSERT(ptr);
  DBUG_ASSERT(object_p);

  /* Skip leading space. */
  token= skip_space(ptr);

  /*
    Check for quoted search string.
    If unquoted, search for space or terminator.
  */
  quote= terminator;
  switch (*token) {
  case '\'':
  case '\"':
  case '`':
    quote= *(token++);
  }

  /* Find end of token. if (quote != terminator), token is quoted. */
  tend= token_end(token, quote, (quote != terminator));

  /* If quoted, check that we found the quote. */
  if ((quote != terminator) && (*tend != quote))
  {
    errm("Cannot parse search name, improperly quoted.\n");
    goto end;
  }

  /* Copy token into object name. */
  *object_p= (char*) my_memdup(token, (size_t) (tend - token + 1),
                               MYF(MY_WME));
  if (*object_p)
    (*object_p)[tend - token]= '\0';

  /* If quoted, skip quote. But don't go past end of string. */
  if ((quote != terminator) && *tend)
    tend++;

  /* Skip trailing space. */
  ptr= skip_space(tend);

 end:
  return ptr;
}


/**
  Parse the search name.

  Split search name into database name, if present, and object name.

  @param[in]    search_name             search name
  @param[out]   search_database_name_p  database name
  @param[out]   search_object_name_p    object name

  @return       status
    @retval     0                       ok
    @retval     != 0                    error
*/

static int
parse_search_name(const char *search_name,
                  char **search_database_name_p,
                  char **search_object_name_p)
{
  char          *database= NULL;
  char          *object= NULL;
  const char    *ptr;
  int           status= 1; /* Assume error. */
  DBUG_ENTER("parse_search_name");
  DBUG_ASSERT(search_name);
  DBUG_ASSERT(search_database_name_p);
  DBUG_ASSERT(search_object_name_p);
  DBUG_PRINT("mysqlbackup", ("searching for '%s'\n", search_name));

  /*
    Extract object name.
    Unless quoted, it is terminated by '.' or '\0' or space.
  */
  ptr= extract_object_name(search_name, '.', &object);
  if (!object)
    goto end;

  /* If object name was terminated by '.', a second name follows. */
  if (*ptr == '.')
  {
    /* Yes, so object name was a database name. */
    database= object;
    object= NULL;

    /* Skip dot. */
    ptr++;

    /*
      Extract object name.
      Unless quoted, it is terminated by '\0' or space.
    */
    ptr= extract_object_name(ptr, '\0', &object);
    if (!object)
      goto end;
  }

  /* Now we must be at string end. */
  if (*ptr)
  {
    errm("Cannot parse search name, unrecognized syntax at '%s'\n", ptr);
    goto end;
  }

  /* Everything looks fine. */
  *search_database_name_p= database;
  *search_object_name_p= object;
  status= 0;

 end:
  if (status)
  {
    my_free(object, MYF(MY_ALLOW_ZERO_PTR));
    my_free(database, MYF(MY_ALLOW_ZERO_PTR));
  }
  DBUG_RETURN(status);
}


/**
  Initialize application.

  Read options from config file and command line. Do basic set up.

  @param[in]    argc            argument count
  @param[in]    argv            argument vector

  @return       status
    @retval     0               ok
    @retval     != 0            error
*/

static int
init_client(int *argc_p, char ***argv_p)
{
  int                           rc= 1; /* Assume usage error. */
  DBUG_ENTER("init_client");

  /* Set program name for debugging. */
  DBUG_PROCESS((*argv_p)[0]);

  /* Initialize time functions. */
  my_init_time();

  /* Load options from config file(s). */
  load_defaults("my", load_default_groups, argc_p, argv_p);

  /* Save a pointer to the arguments allocated by load_defaults(). */
  defaults_argv= *argv_p;

  /* Evaluate command line options. */
  if (handle_options(argc_p, argv_p, my_long_options, get_one_option))
  {
    /* Error message reported by mysys. */
    goto use_err;
  }

  /* We need exactly one argument. */
  if (*argc_p != 1)
  {
    errm("incorrect number of arguments.\n");
    goto use_err;
  }

  /* Evaluate search option. */
  if (opt_mysqlbackup_search)
  {
    if (parse_search_name(opt_mysqlbackup_search, &search_database_name,
                          &search_object_name))
    {
      goto use_err;
    }
  }

  /* Set open files limit. */
  my_set_max_open_files(opt_open_files_limit);

  /* Success. */
  rc= 0;
  goto end;

 use_err:
  usage();

 end:
  DBUG_RETURN(rc);
}


/**
  De-initialize application.
*/

static void
cleanup_client(void)
{
  my_free(search_object_name, MYF(MY_ALLOW_ZERO_PTR));
  my_free(search_database_name, MYF(MY_ALLOW_ZERO_PTR));

#ifdef MYSQL_CLIENT
  my_free((char*) opt_database, MYF(MY_ALLOW_ZERO_PTR));
  my_free((char*) opt_host, MYF(MY_ALLOW_ZERO_PTR));
  my_free((char*) opt_pass, MYF(MY_ALLOW_ZERO_PTR));
  my_free((char*) opt_user, MYF(MY_ALLOW_ZERO_PTR));
#endif

  if (defaults_argv)
    free_defaults(defaults_argv);

  my_free_open_file_info();
}


/**
  Print a formatted time string.

  @param[in]    time            time value
*/

static void
print_time(bstream_time_t *time)
{
  DBUG_ASSERT(time);
  printf("%04d-%02d-%02d %02d:%02d:%02d UTC",
         time->year + 1900, time->mon + 1, time->mday,
         time->hour, time->min, time->sec);
}


/**
  Create a human readable number of bytes.

  Numbers are shown as +-0..16384 with a postfix of "bytes",
  or as +-16..16384 with a postfix of "KB", "MB", "GB", "TB", "PB", or "EB".
  If one day longlong is > 64 bit, there can be higher numbers with "EB".
  The range is not +-0..1024 or +-1..1024 because we would lose too much
  accuracy. Using +-16..16384, we have at least two significant digits.
  And, by chance, 16384 EB - 1 is exactly the highest possible value in a
  64-bit value (if used unsigned, which we don't do here).

  @param[in]        value           number value
  @param[in,out]    buff            result buffer, filled by this function

  @return           buff            "echo" the input argument
*/

static char*
llstr_human(longlong value, char *buff)
{
  const char *postfixes[]= {"bytes", "KB", "MB", "GB", "TB", "PB", "EB", NULL};
  const char **postfix_p= postfixes;
  const char *src;
  char       *dst;
  DBUG_ASSERT(buff);

  if (!opt_mysqlbackup_exact)
  {
    /* Shrink value while it's > 16K and postfix list is not at end. */
    while (((value > 16383) || (value < -16384)) && *(++postfix_p))
      value/= 1024;
    /*
      In case that we terminated the loop due to end of the postfixes
      list, we need to compensate the loop's ++postfix_p so that we
      point to the last non-NULL postfix. Note that this can only happen
      on machines where longlong has more than 64 bits.
    */
    if (!*postfix_p)
      --postfix_p; /* purecov: inspected */
  }
  /* Convert the number into a string. */
  (void) llstr(value, buff);
  /* Find end of destination string. */
  for (dst= buff; *dst; dst++) {}
  /* Get pointer to the source string. */
  src= *postfix_p;
  /* Append postfix, including the terminating '\0'. */
  *(dst++)= ' ';
  while ((*(dst++)= *(src++))) {}
  /* Result is in buff. */
  return buff;
}


/**
  Get per-db item type.

  @param[in]    item            item reference

  @return       type name
*/

static const char*
get_perdb_item_type(struct st_bstream_item_info *item)
{
  const char *type_s;
  DBUG_ENTER("get_perdb_item_type");
  DBUG_ASSERT(item);

  switch (item->type) {
  case BSTREAM_IT_PRIVILEGE:
    type_s= "Privilege";
    break;
  case BSTREAM_IT_VIEW:
    type_s= "View";
    break;
  case BSTREAM_IT_SPROC:
    type_s= "Sproc";
    break;
  case BSTREAM_IT_SFUNC:
    type_s= "Sfunc";
    break;
  case BSTREAM_IT_EVENT:
    type_s= "Event";
    break;
  case BSTREAM_IT_TRIGGER:
    type_s= "Trigger";
    break;
  default:
    /* purecov: begin deadcode */
    DBUG_ASSERT(0);
    type_s= "Unknown item type";
    break;
    /* purecov: end */
  }
  DBUG_RETURN(type_s);
}


/**
  Get item information.

  @param[in]    item            item reference
  @param[out]   name_p          item name reference
  @param[out]   db_name_p       database name reference
  @param[out]   mdata_p         meta data reference

  @return       type name
*/

static const char*
get_item_info(struct st_bstream_item_info *item, struct st_blob **name_p,
              struct st_blob **db_name_p, struct st_backup_metadata **mdata_p)
{
  const char *type_s;
  DBUG_ENTER("get_item_info");
  DBUG_ASSERT(item);
  DBUG_ASSERT(name_p);
  DBUG_ASSERT(db_name_p);
  DBUG_ASSERT(mdata_p);

  *name_p= &item->name;

  switch (item->type) {

  case BSTREAM_IT_CHARSET:
  case BSTREAM_IT_USER:
  case BSTREAM_IT_TABLESPACE:
  {
    struct st_backup_global *bup_global= (struct st_backup_global*) item;
    type_s= bup_global->glb_typename;
    *mdata_p= &bup_global->glb_metadata;
    break;
  }

  case BSTREAM_IT_DB:
  {
    struct st_backup_database *bup_database= (struct st_backup_database*) item;
    type_s= "Database";
    *mdata_p= &bup_database->db_metadata;
    break;
  }

  case BSTREAM_IT_TABLE:
  {
    struct st_backup_table *bup_table= (struct st_backup_table*) item;
    type_s= "Table";
    *mdata_p= &bup_table->tbl_metadata;
    *db_name_p= &bup_table->tbl_item.base.db->base.name;
    break;
  }

  case BSTREAM_IT_PRIVILEGE:
  case BSTREAM_IT_VIEW:
  case BSTREAM_IT_SPROC:
  case BSTREAM_IT_SFUNC:
  case BSTREAM_IT_EVENT:
  case BSTREAM_IT_TRIGGER:
  {
    struct st_backup_perdb *bup_perdb= (struct st_backup_perdb*) item;

    type_s= get_perdb_item_type(item);
    *mdata_p= &bup_perdb->perdb_metadata;
    *db_name_p= &bup_perdb->perdb_item.db->base.name;
    break;
  }

  default:
  {
    /* purecov: begin deadcode */
    DBUG_ASSERT(0);
    type_s= "Unknown item type";
    break;
    /* purecov: end */
  }
  }
  DBUG_PRINT("mysqlbackup", ("%s", type_s));
  DBUG_RETURN(type_s);
}


/**
  Print item name.

  @param[in]        what            flags describing what to print
                                    PRI_NAME        item name
                                    PRI_META        meta data
                                    PRI_SPACE       empty line
  @param[in]        type_s          type name
  @param[in]        name            item name
  @param[in]        db_name         database name, may be NULL
  @param[in]        indent          number of columns to indent
  @param[in,out]    space_printed   if empty line has been printed
*/

static void
print_item_name(uint what, const char *type_s,
                struct st_blob *name, struct st_blob *db_name,
                uint indent, bool *space_printed)
{
  DBUG_ENTER("print_item_name");
  DBUG_ASSERT(type_s);
  DBUG_ASSERT(name);
  /* db_name may be NULL. */
  DBUG_ASSERT(space_printed);

  if (what & PRI_NAME)
  {
    if ((what & PRI_SPACE) && !*space_printed)
    {
      /* purecov: begin inspected */
      printf("\n");
      *space_printed= TRUE;
      /* purecov: end */
    }
    if (db_name)
      printf("%*s%-9s '%.*s'.'%.*s'\n",
             indent, "", type_s,
             BBLS(db_name), BBLS(name));
    else
      printf("%*s%-9s '%.*s'\n",
             indent, "", type_s,
             BBLS(name));
  }
  DBUG_VOID_RETURN;
}


/**
  Print item meta data.

  @param[in]        what            flags describing what to print
                                    PRI_NAME        item name
                                    PRI_META        meta data
                                    PRI_SPACE       empty line
  @param[in]        type_s          type name
  @param[in]        name            item name
  @param[in]        db_name         database name, may be NULL
  @param[in]        mdata           meta data reference, may be NULL
  @param[in]        indent          number of columns to indent
  @param[in,out]    space_printed   if empty line has been printed
*/

static void
print_item_metadata(uint what, const char *type_s,
                    struct st_blob *name, struct st_blob *db_name,
                    struct st_backup_metadata *mdata,
                    uint indent, bool *space_printed)
{
  DBUG_ENTER("print_item_metadata");
  DBUG_ASSERT(type_s);
  DBUG_ASSERT(name);
  /* db_name may be NULL. */
  /* mdata may be NULL. */
  DBUG_ASSERT(space_printed);

  if ((what & PRI_META) && mdata)
  {
    if (opt_mysqlbackup_metadata_statements)
    {
      if ((what & PRI_SPACE) && !*space_printed)
      {
        printf("\n");
        *space_printed= TRUE;
      }
      if (db_name)
        printf("%*s%s '%.*s'.'%.*s' statement: '%.*s'\n",
               indent, "", type_s,
               BBLS(db_name), BBLS(name), BBLS(&mdata->md_query));
      else
        printf("%*s%s '%.*s' statement: '%.*s'\n",
               indent, "", type_s,
               BBLS(name), BBLS(&mdata->md_query));
    }

    if (opt_mysqlbackup_metadata_extra &&
        (BBL(&mdata->md_data) || opt_verbose))
    {
      if ((what & PRI_SPACE) && !*space_printed)
      {
        printf("\n");
        *space_printed= TRUE;
      }
      if (db_name)
        printf("%*s%s '%.*s'.'%.*s' extra data length: %lu\n",
               indent, "", type_s,
               BBLS(db_name), BBLS(name), BBL(&mdata->md_data));
      else
        printf("%*s%s '%.*s' extra data length: %lu\n",
               indent, "", type_s,
               BBLS(name), BBL(&mdata->md_data));
    }
  }
  DBUG_VOID_RETURN;
}


/**
  Print item information.

  @param[in]    indent          number of columns to indent
  @param[in]    item            reference to the item
  @param[in]    what            flags describing what to print
                                PRI_NAME        item name
                                PRI_META        meta data
                                PRI_SPACE       empty line
*/

static void
print_item(uint indent, struct st_bstream_item_info *item, uint what)
{
  struct st_blob                *name;
  struct st_blob                *db_name= NULL;
  struct st_backup_metadata     *mdata= NULL;
  const char                    *type_s;
  bool                          space_printed= FALSE;
  DBUG_ENTER("print_item");
  DBUG_ASSERT(item);
  DBUG_PRINT("mysqlbackup", ("item: 0x%lx", (long) item));
  DBUG_PRINT("mysqlbackup", ("type: %u", item->type));

  type_s= get_item_info(item, &name, &db_name, &mdata);

  print_item_name(what, type_s, name, db_name,
                  indent, &space_printed);

  print_item_metadata(what, type_s, name, db_name, mdata,
                      indent, &space_printed);

  IF_DBUG(fflush(stdout));
  DBUG_VOID_RETURN;
}


/**
  Match an object name against a search pattern.

  @param[in]    name                    object name
  @param[in]    pattern                 search pattern
  @param[in]    pattern_length          length of search pattern

  @return       match
    @retval     FALSE                   name does not match pattern
    @retval     TRUE                    name matches pattern
*/

static bool
match_name_against_pattern(struct st_blob *name, char *pattern,
                           uint pattern_length)
{
  return !my_wildcmp(&my_charset_latin1,
                     (char*) name->begin, (char*) name->end,
                     pattern, pattern + pattern_length,
                     '\\', '_', '%');
}


/**
  Print an object if its name matches a search pattern.

  @param[in]    name                    object name
  @param[in]    pattern                 search pattern
  @param[in]    pattern_length          length of search pattern
  @param[in]    item                    reference to some item
  @param[in]    indent                  number of columns to indent
*/

static void
print_if_match(struct st_blob *name, char *pattern, uint pattern_length,
               struct st_bstream_item_info *item, uint indent)
{
  if (match_name_against_pattern(name, pattern, pattern_length))
    print_item(indent, item, PRI_NAME | PRI_META);
}


/**
  Search backup object in the catalog.

  Prints the items it finds according to command line options
  like --metadata-statements.

  @param[in]    bup_catalog             catalog reference
  @param[in]    search_database_name    database name, may be NULL
  @param[in]    search_object_name      object name
*/

static void
search_objects(struct st_backup_catalog *bup_catalog,
               char *search_database_name,
               char *search_object_name)
{
  uint search_database_name_len;
  uint search_object_name_len;
  uint idx;
  DBUG_ENTER("search_objects");
  DBUG_ASSERT(bup_catalog);
  /* search_database_name may be NULL. */
  DBUG_ASSERT(search_object_name);

  printf("\n");
  if (search_database_name)
    printf("Searching for '%s'.'%s'\n", search_database_name,
           search_object_name);
  else
    printf("Searching for '%s'\n", search_object_name);

  if (search_database_name)
    search_database_name_len= strlen(search_database_name);
  search_object_name_len= strlen(search_object_name);

  if (opt_mysqlbackup_image_order)
  {
    for (idx= 0; idx < bup_catalog->cat_image_ordered_items.elements; idx++)
    {
      struct st_bstream_item_info       *item;
      struct st_blob                    *name;

      /* Note that the array contains pointers only. */
      item= *((struct st_bstream_item_info**)
              dynamic_array_ptr(&bup_catalog->cat_image_ordered_items, idx));
      name= &item->name;
      switch (item->type) {
      case BSTREAM_IT_CHARSET:
      case BSTREAM_IT_USER:
      case BSTREAM_IT_TABLESPACE:
      case BSTREAM_IT_DB:
        {
          print_if_match(name, search_object_name, search_object_name_len,
                         item, 2);
          break;
        }
      case BSTREAM_IT_TABLE:
      case BSTREAM_IT_PRIVILEGE:
      case BSTREAM_IT_VIEW:
      case BSTREAM_IT_SPROC:
      case BSTREAM_IT_SFUNC:
      case BSTREAM_IT_EVENT:
      case BSTREAM_IT_TRIGGER:
        {
          struct st_blob *db_name;

          db_name= &((st_bstream_dbitem_info*)item)->db->base.name;
          if (match_name_against_pattern(db_name, search_database_name,
                                         search_database_name_len))
            print_if_match(name, search_object_name, search_object_name_len,
                           item, 2);
          break;
        }
        /* purecov: begin deadcode */
      default:
        DBUG_ASSERT(0);
        break;
        /* purecov: end */
      }
    }
  }
  else
  {
    if (!search_database_name)
    {
      DYNAMIC_ARRAY **search_array_p;
      DYNAMIC_ARRAY *search_array_array[]= {
        &bup_catalog->cat_charsets,
        &bup_catalog->cat_users,
        &bup_catalog->cat_tablespaces,
        &bup_catalog->cat_databases,
        NULL
      };

      /*
        Search global items.
        Note that we put pointers to different object types to the
        arrays. It is somewhat unclean, to dereference to a different
        pointer type here. This works only because all objects start
        with a struct st_bstream_item_info. So the pointer to the
        original object is at the same time also the pointer to the
        item within it. Taking this way allows to have one loop for
        all global objects.
      */
      for (search_array_p= search_array_array;
           *search_array_p;
           search_array_p++)
      {
        /* Search global item. */
        for (idx= 0; idx < (*search_array_p)->elements; idx++)
        {
          struct st_bstream_item_info   *item;
          struct st_blob                *name;

          /* Note that the array contains pointers only. */
          item= *((struct st_bstream_item_info**)
                  dynamic_array_ptr(*search_array_p, idx));
          name= &item->name;
          print_if_match(name, search_object_name, search_object_name_len,
                         item, 2);
        }
      }
      goto end;
    }

    /* Search object within databases. */
    for (idx= 0; idx < bup_catalog->cat_databases.elements; idx++)
    {
      struct st_backup_database *bup_database;
      struct st_blob            *db_name;

      /* Note that the array contains pointers only. */
      bup_database= *((struct st_backup_database**)
                      dynamic_array_ptr(&bup_catalog->cat_databases, idx));
      db_name= &bup_database->db_item.base.name;
      if (match_name_against_pattern(db_name, search_database_name,
                                     search_database_name_len))
      {
        DYNAMIC_ARRAY **search_array_p;
        DYNAMIC_ARRAY *search_array_array[]= {
          &bup_database->db_tables,
          &bup_database->db_perdbs,
          NULL
        };
        uint jdx;

        /* Search per-db items. */
        for (search_array_p= search_array_array;
             *search_array_p;
             search_array_p++)
        {
          /* Search per-db item. */
          for (jdx= 0; jdx < (*search_array_p)->elements; jdx++)
          {
            struct st_bstream_item_info *item;
            struct st_blob              *name;

            /* Note that the array contains pointers only. */
            item= *((struct st_bstream_item_info**)
                    dynamic_array_ptr(*search_array_p, jdx));
            name= &item->name;
            print_if_match(name, search_object_name, search_object_name_len,
                           item, 4);
          }
        }
      }
    }
  }

 end:
  DBUG_VOID_RETURN;
}


/**
  Print backup image header.

  @param[in]    bup_catalog             catalog reference
*/

static void
print_header(struct st_backup_catalog *bup_catalog)
{
  struct st_bstream_image_header *hdr= &bup_catalog->cat_header;
  char llbuff[22];
  DBUG_ASSERT(bup_catalog);

  printf("\n");
  printf("Image path:          '%s'\n",
         bup_catalog->cat_image_path);
  printf("Image size:          %s\n",
         llstr_human(bup_catalog->cat_image_size, llbuff));
  printf("Image compression:   %s\n", bup_catalog->cat_zalgo);
  printf("Image version:       %u\n", hdr->version);
  printf("Creation time:       ");
  print_time(&hdr->start_time);
  printf("\n");
  printf("Server version:      %d.%d.%d (%.*s)\n",
         hdr->server_version.major, hdr->server_version.minor,
         hdr->server_version.release, BBLS(&hdr->server_version.extra));
  printf("Server byte order:   %s\n",
         hdr->flags & BSTREAM_FLAG_BIG_ENDIAN ?
         "big-endian" : "little-endian");
  if (hdr->flags && opt_verbose)
  {
    /*
      Print options as a comma separated list. The separator before the
      first option is empty. After an options is printed, the separator
      changes to a comma.
    */
    const char  *separator= "";
    const char  *comma= ", ";

    printf("Image options:       ");
    if (hdr->flags & BSTREAM_FLAG_INLINE_SUMMARY)
    {
      /* purecov: begin inspected */
      printf("%sINLINE_SUMMARY", separator);
      separator= comma;
      /* purecov: end */
    }
    if (hdr->flags & BSTREAM_FLAG_BIG_ENDIAN)
    {
      /* purecov: begin inspected */
      printf("%sBIG_ENDIAN", separator);
      separator= comma;
      /* purecov: end */
    }
    if (hdr->flags & BSTREAM_FLAG_BINLOG)
    {
      printf("%sBINLOG", separator);
      separator= comma;
    }
    printf("\n");
  }
}


/**
  Print backup summary.

  @param[in]    bup_catalog             catalog reference
*/

static void
print_summary(struct st_backup_catalog *bup_catalog)
{
  struct st_bstream_image_header *hdr= &bup_catalog->cat_header;
  DBUG_ASSERT(bup_catalog);

  if (opt_mysqlbackup_summary)
  {
    printf("\n");
    printf("Summary:\n");
    printf("\n");
    printf("Creation time:       ");
    print_time(&hdr->start_time);
    printf("\n");
    printf("Validity time:       ");
    print_time(&hdr->vp_time);
    printf("\n");
    printf("Finish   time:       ");
    print_time(&hdr->end_time);
    printf("\n");

    if (hdr->flags & BSTREAM_FLAG_BINLOG)
    {
      printf("Binlog coordinates:  %s:%lu\n",
             hdr->binlog_pos.file ? hdr->binlog_pos.file : "[NULL]",
             hdr->binlog_pos.pos);
      printf("Binlog group coords: %s:%lu\n",
             hdr->binlog_group.file ? hdr->binlog_group.file : "[NULL]",
             hdr->binlog_group.pos);
    }
    else
    {
      printf("No binlog information\n");
    }
  }
}


/**
  Read and print summary.

  @param[in]    strm                    image handle reference
  @param[in]    bup_catalog             catalog reference

  @return       status
    @retval     BSTREAM_OK              ok
    @retval     != BSTREAM_OK           error
*/

static enum enum_bstream_ret_codes
read_and_print_summary(struct st_stream *strm,
                       struct st_backup_catalog *bup_catalog)
{
  enum enum_bstream_ret_codes brc;
  DBUG_ENTER("read_and_print_summary");
  DBUG_ASSERT(strm);
  DBUG_ASSERT(bup_catalog);

  /*
    Read summary.
  */
  brc= backup_read_summary(strm, bup_catalog);
  if (brc != BSTREAM_OK)
  {
    goto end;
  }

  /*
    Print summary.
  */
  print_summary(bup_catalog);

 end:
  DBUG_RETURN(brc);
}


/**
  Print special character sets.

  It has been decided not to display the first two character sets from
  the catalog with the other catalog items.

  The first two character sets are:
  1: Character set to use for interpreting the backup file.
  2: Server character set.

  These do not count as catalog items though they are transported with
  the catalog. They are to be printed in the header section.

  @param[in]    bup_catalog             catalog reference
*/

static void
print_special_charsets(struct st_backup_catalog *bup_catalog)
{
  struct st_backup_global *bup_global;
  DBUG_ASSERT(bup_catalog);

  if (bup_catalog->cat_charsets.elements >= 2)
  {
    /* Note that the array contains pointers only. */
    bup_global= *((struct st_backup_global**)
                  dynamic_array_ptr(&bup_catalog->cat_charsets, 1));
    printf("Server charset:      '%.*s'\n", BBLS(&bup_global->glb_item.name));
  }

  if (opt_verbose && (bup_catalog->cat_charsets.elements >= 1))
  {
    /* Note that the array contains pointers only. */
    bup_global= *((struct st_backup_global**)
                  dynamic_array_ptr(&bup_catalog->cat_charsets, 0));
    printf("Backup image chrset: '%.*s'\n", BBLS(&bup_global->glb_item.name));
  }
}


/**
  Print tables contained in a snapshot.

  @param[in]    bup_catalog             catalog reference
  @param[in]    snapshot                snapshot reference
*/

static void
print_snapshot_tables(struct st_backup_catalog *bup_catalog,
                      struct st_bstream_snapshot_info *snapshot,
                      uint snap_num)
{
  DBUG_ASSERT(bup_catalog);

  /*
    If there is a table, there must also be a database.
    We require to read the catalog, if snapshots are to be printed.
  */
  DBUG_ASSERT(!snapshot->table_count ||
              bup_catalog->cat_databases.elements);
  if (snapshot->table_count)
  {
    uint idx;

    for (idx= 0; idx < bup_catalog->cat_databases.elements; idx++)
    {
      struct st_backup_database   *bup_database;
      struct st_blob *db_name;

      /* Note that the array contains pointers only. */
      bup_database= *((struct st_backup_database**)
                      dynamic_array_ptr(&bup_catalog->cat_databases, idx));
      db_name= &bup_database->db_item.base.name;
      if (bup_database->db_tables.elements)
      {
        struct st_backup_table  *bup_table;
        uint                    jdx;

        for (jdx= 0; jdx < bup_database->db_tables.elements; jdx++)
        {
          struct st_blob *name;

          /* Note that the array contains pointers only. */
          bup_table= *((struct st_backup_table**)
                       dynamic_array_ptr(&bup_database->db_tables, jdx));
          if (bup_table->tbl_item.snap_num == snap_num)
          {
            name= &bup_table->tbl_item.base.base.name;
            printf("  Snapshot %u table   '%.*s'.'%.*s'\n",
                   snap_num, BBLS(db_name), BBLS(name));
          }
        }
      }
    }
  }
}


/**
  Print snapshot information.

  @param[in]    bup_catalog             catalog reference
*/

static void
print_snapshots(struct st_backup_catalog *bup_catalog)
{
  struct st_bstream_image_header *hdr= &bup_catalog->cat_header;
  uint idx;
  DBUG_ASSERT(bup_catalog);

  if (opt_mysqlbackup_snapshots)
  {
    printf("Snapshot count:      %u\n", hdr->snap_count);
    printf("\n");
    printf("Snapshots:\n");
    printf("\n");

    DBUG_ASSERT(hdr->snap_count == bup_catalog->cat_snapshots.elements);
    DBUG_ASSERT(hdr->snap_count <=
                sizeof(hdr->snapshot) / sizeof(hdr->snapshot[0]));

    for (idx= 0; idx < hdr->snap_count; idx++)
    {
      struct st_bstream_snapshot_info     *snapshot= hdr->snapshot + idx;
      const char                          *snap_type;

      switch (snapshot->type) {
      case BI_NATIVE:     snap_type= "native from";
        break;
      case BI_DEFAULT:    snap_type= "logical from locked tables";
        break;
      case BI_CS:         snap_type= "logical from consistent snapshot";
        break;
      /* purecov: begin inspected */
      case BI_NODATA:     snap_type= "nodata";
        break;
      default:            snap_type= "unknown/illegal";
        break;
      /* purecov: end */
      }
      if (snapshot->type == BI_NATIVE)
        printf("  Snapshot %u type    %s '%.*s'  version %u.%u\n",
               idx, snap_type,
               BBLS(&snapshot->engine.name),
               snapshot->engine.major, snapshot->engine.minor);
      else
        printf("  Snapshot %u type    %s\n", idx, snap_type);

      printf("  Snapshot %u version %u\n", idx, snapshot->version);

      if (snapshot->options)
        printf("  Snapshot %u options 0x%04x\n",
               idx, snapshot->options); /* purecov: inspected */

      printf("  Snapshot %u tables  %lu\n", idx, snapshot->table_count);

      /* List tables included in this snapshot. */
      print_snapshot_tables(bup_catalog, snapshot, idx);
    }
  }
}


/**
  Print catalog summary.

  @param[in]    bup_catalog             catalog reference
*/

static void
print_catalog_summary(struct st_backup_catalog *bup_catalog)
{
  DBUG_ASSERT(bup_catalog);

  if (opt_mysqlbackup_catalog_summary)
  {
    ulong sum_table;
    ulong sum_perdb;

    printf("\n");
    printf("Catalog summary:\n");
    printf("\n");
    if (opt_verbose && (bup_catalog->cat_charsets.elements > 2))
    {
      /*
        The first two character sets are special:
        1. Character set to use for interpreting the backup file.
        2. Server character set.
        These do not count as catalog items.
      */
      /* purecov: begin inspected */
      printf("  Character sets:         %u\n",
             bup_catalog->cat_charsets.elements - 2);
      /* purecov: end */
    }

    if (bup_catalog->cat_users.elements)
    {
      /* purecov: begin inspected */
      printf("  Users:                  %u\n",
             bup_catalog->cat_users.elements);
      /* purecov: end */
    }

    if (bup_catalog->cat_tablespaces.elements)
    {
      printf("  Tablespaces:            %u\n",
             bup_catalog->cat_tablespaces.elements);
    }

    if (bup_catalog->cat_databases.elements)
    {
      uint idx;

      printf("  Databases:              %u\n",
             bup_catalog->cat_databases.elements);
      sum_table= 0;
      sum_perdb= 0;
      for (idx= 0; idx < bup_catalog->cat_databases.elements; idx++)
      {
        struct st_backup_database   *bup_database;

        /* Note that the array contains pointers only. */
        bup_database= *((struct st_backup_database**)
                        dynamic_array_ptr(&bup_catalog->cat_databases, idx));
        sum_table+= bup_database->db_tables.elements;
        sum_perdb+= bup_database->db_perdbs.elements;
      }
      if (sum_table)
        printf("  Tables:                 %lu\n", sum_table);
      if (sum_perdb)
        printf("  Non-table db objects:   %lu\n", sum_perdb);
    }
  }
}


/**
  Print catalog details.

  @param[in]    bup_catalog             catalog reference
*/

static void
print_catalog_details(struct st_backup_catalog *bup_catalog)
{
  DBUG_ASSERT(bup_catalog);

  if (opt_mysqlbackup_catalog_details)
  {
    struct st_backup_global     *bup_global;
    struct st_backup_database   *bup_database;
    uint idx;

    printf("\n");
    printf("Catalog details:\n");
    printf("\n");

    if (opt_mysqlbackup_image_order)
    {
      for (idx= 0; idx < bup_catalog->cat_image_ordered_items.elements; idx++)
      {
        struct st_bstream_item_info *item;

        /* Note that the array contains pointers only. */
        item= *((struct st_bstream_item_info**)
                dynamic_array_ptr(&bup_catalog->cat_image_ordered_items, idx));
        print_item(2, item, PRI_NAME);
      }
    }
    else
    {
      /* Charsets. */
      if (opt_verbose)
      {
        /*
          The first two character sets are special:
          1. Character set to use for interpreting the backup file.
          2. Server character set.
          These do not count as catalog items.
        */
        for (idx= 2; idx < bup_catalog->cat_charsets.elements; idx++)
        {
          /* purecov: begin inspected */
          /* Note that the array contains pointers only. */
          bup_global= *((struct st_backup_global**)
                        dynamic_array_ptr(&bup_catalog->cat_charsets, idx));
          print_item(2, (struct st_bstream_item_info*) bup_global, PRI_NAME);
          /* purecov: end */
        }
      }

      /* Users. */
      for (idx= 0; idx < bup_catalog->cat_users.elements; idx++)
      {
        /* purecov: begin inspected */
        /* Note that the array contains pointers only. */
        bup_global= *((struct st_backup_global**)
                      dynamic_array_ptr(&bup_catalog->cat_users, idx));
        print_item(2, (struct st_bstream_item_info*) bup_global, PRI_NAME);
        /* purecov: end */
      }

      /* Table spaces. */
      for (idx= 0; idx < bup_catalog->cat_tablespaces.elements; idx++)
      {
        /* Note that the array contains pointers only. */
        bup_global= *((struct st_backup_global**)
                      dynamic_array_ptr(&bup_catalog->cat_tablespaces, idx));
        print_item(2, (struct st_bstream_item_info*) bup_global, PRI_NAME);
      }

      /* Databases. */
      for (idx= 0; idx < bup_catalog->cat_databases.elements; idx++)
      {
        /* Note that the array contains pointers only. */
        bup_database= *((struct st_backup_database**)
                        dynamic_array_ptr(&bup_catalog->cat_databases, idx));
        print_item(2, (struct st_bstream_item_info*) bup_database, PRI_NAME);

        /* Tables. */
        if (bup_database->db_tables.elements)
        {
          struct st_backup_table  *bup_table;
          uint                    jdx;

          for (jdx= 0; jdx < bup_database->db_tables.elements; jdx++)
          {
            /* Note that the array contains pointers only. */
            bup_table= *((struct st_backup_table**)
                         dynamic_array_ptr(&bup_database->db_tables, jdx));
            print_item(4, (struct st_bstream_item_info*) bup_table, PRI_NAME);
          }
        }

        /* Perdbs. */
        if (bup_database->db_perdbs.elements)
        {
          struct st_backup_perdb  *bup_perdb;
          uint                    jdx;

          for (jdx= 0; jdx < bup_database->db_perdbs.elements; jdx++)
          {
            /* Note that the array contains pointers only. */
            bup_perdb= *((struct st_backup_perdb**)
                         dynamic_array_ptr(&bup_database->db_perdbs, jdx));
            print_item(4, (struct st_bstream_item_info*) bup_perdb, PRI_NAME);
          }
        }
      }
    }
  }
}


/**
  Print meta data.

  @param[in]    bup_catalog             catalog reference
*/

static void
print_metadata(struct st_backup_catalog *bup_catalog)
{
  DBUG_ASSERT(bup_catalog);

  if (opt_mysqlbackup_metadata_statements || opt_mysqlbackup_metadata_extra)
  {
    uint idx;

    printf("\n");
    printf("Meta data:\n");

    if (opt_mysqlbackup_image_order)
    {
      for (idx= 0;
           idx < bup_catalog->cat_image_ordered_metadata.elements;
           idx++)
      {
        struct st_bstream_item_info *item;

        /* Note that the array contains pointers only. */
        item= *((struct st_bstream_item_info**)
                dynamic_array_ptr(&bup_catalog->cat_image_ordered_metadata,
                                  idx));
        print_item(2, item, PRI_META | PRI_SPACE);
      }
    }
    else
    {
      /* Charsets don't have meta data. */
      /* Users don't have meta data. */

      /* Tablespaces. */
      for (idx= 0; idx < bup_catalog->cat_tablespaces.elements; idx++)
      {
        struct st_backup_global *bup_global;

        /* Note that the array contains pointers only. */
        bup_global= *((struct st_backup_global**)
                      dynamic_array_ptr(&bup_catalog->cat_tablespaces, idx));
        print_item(2, (struct st_bstream_item_info*) bup_global,
                   PRI_META | PRI_SPACE);
      }

      /* Databases and their items. */
      for (idx= 0; idx < bup_catalog->cat_databases.elements; idx++)
      {
        struct st_backup_database *bup_database;

        /* Note that the array contains pointers only. */
        bup_database= *((struct st_backup_database**)
                        dynamic_array_ptr(&bup_catalog->cat_databases, idx));
        print_item(2, (struct st_bstream_item_info*) bup_database,
                   PRI_META | PRI_SPACE);

        /* Tables. */
        {
          struct st_backup_table  *bup_table;
          uint                    jdx;

          for (jdx= 0; jdx < bup_database->db_tables.elements; jdx++)
          {
            /* Note that the array contains pointers only. */
            bup_table= *((struct st_backup_table**)
                         dynamic_array_ptr(&bup_database->db_tables, jdx));
            print_item(4, (struct st_bstream_item_info*) bup_table,
                       PRI_META | PRI_SPACE);
          }
        }

        /* Perdb items. */
        {
          struct st_backup_perdb  *bup_perdb;
          uint                    jdx;

          for (jdx= 0; jdx < bup_database->db_perdbs.elements; jdx++)
          {
            /* Note that the array contains pointers only. */
            bup_perdb= *((struct st_backup_perdb**)
                         dynamic_array_ptr(&bup_database->db_perdbs, jdx));
            print_item(4, (struct st_bstream_item_info*) bup_perdb,
                       PRI_META | PRI_SPACE);
          }
        }
      }
    }
  }
}


/**
  Print table data.

  At the moment it does only print size of data.

  @param[in]    bup_catalog             catalog reference
  @param[in]    data_chunk              chunk reference
  @param[in]    chunk_cnt               number of chunk in backup image
*/

static void
print_table_data(struct st_backup_catalog *bup_catalog,
                 struct st_bstream_data_chunk *data_chunk,
                 ulonglong chunk_cnt)
{
  struct st_backup_table              *bup_table;
  struct st_blob                      *tbl_name;
  struct st_blob                      *db_name;
  char                                llbuff[22];

  /*
    There can be a special "table" with number 0: "common data".
    Normal tables start at number 1.
  */
  if (data_chunk->table_num)
  {
    bup_table= backup_locate_table(bup_catalog, data_chunk->snap_num,
                                   data_chunk->table_num - 1);
    /* Accumulate data size. */
    DBUG_ASSERT(!bup_table->tbl_data_size ||
                (bup_table->tbl_item.snap_num == data_chunk->snap_num));
    bup_table->tbl_data_size+= BBL(&data_chunk->data);
    /* Get pointers to the names. */
    tbl_name= &bup_table->tbl_item.base.base.name;
    db_name= &bup_table->tbl_item.base.db->base.name;

    if (opt_mysqlbackup_data_chunks)
    {
      if (opt_mysqlbackup_snapshots)
        printf("  Chunk %s has %lu bytes for table '%.*s'.'%.*s' "
               "from snapshot %d\n",
               llstr(chunk_cnt, llbuff), BBL(&data_chunk->data),
               BBLS(db_name), BBLS(tbl_name), data_chunk->snap_num);
      else
        printf("  Chunk %s has %lu bytes for table '%.*s'.'%.*s'\n",
               llstr(chunk_cnt, llbuff), BBL(&data_chunk->data),
               BBLS(db_name), BBLS(tbl_name));
    }
  }
  else
  {
    /* purecov: begin inspected */
    if (opt_mysqlbackup_data_chunks)
    {
      if (opt_mysqlbackup_snapshots)
        printf("  Chunk %s has %lu bytes for common data "
               "from snapshot %d\n",
               llstr(chunk_cnt, llbuff), BBL(&data_chunk->data),
               data_chunk->snap_num);
      else
        printf("  Chunk %s has %lu bytes for common data\n",
               llstr(chunk_cnt, llbuff), BBL(&data_chunk->data));
    }
    /* purecov: end */
  }
}


/**
  Print table data totals.

  @param[in]    bup_catalog             catalog reference
*/

static void
print_table_totals(struct st_backup_catalog *bup_catalog)
{
  DBUG_ASSERT(bup_catalog);

  if (opt_mysqlbackup_data_totals)
  {
    struct st_backup_database   *bup_database;
    struct st_backup_table      *bup_table;
    struct st_blob              *db_name;
    struct st_blob              *tbl_name;
    uint                        idx;
    uint                        jdx;
    char                        llbuff[22];

    printf("\n");
    printf("Data totals:\n");
    printf("\n");
    for (idx= 0; idx < bup_catalog->cat_databases.elements; idx++)
    {
      /* Note that the array contains pointers only. */
      bup_database= *((struct st_backup_database**)
                      dynamic_array_ptr(&bup_catalog->cat_databases, idx));
      db_name= &bup_database->db_item.base.name;

      for (jdx= 0; jdx < bup_database->db_tables.elements; jdx++)
      {
        /* Note that the array contains pointers only. */
        bup_table= *((struct st_backup_table**)
                     dynamic_array_ptr(&bup_database->db_tables, jdx));
        tbl_name= &bup_table->tbl_item.base.base.name;
        if (opt_mysqlbackup_snapshots)
          printf("  Backup has %s for table '%.*s'.'%.*s' "
                 "in snapshot %d\n",
                 llstr_human(bup_table->tbl_data_size, llbuff),
                 BBLS(db_name), BBLS(tbl_name),
                 bup_table->tbl_item.snap_num);
        else
          printf("  Backup has %s for table '%.*s'.'%.*s'\n",
                 llstr_human(bup_table->tbl_data_size, llbuff),
                 BBLS(db_name), BBLS(tbl_name));
      }
    }
  }
}


/**
  Read and print table data.

  @param[in]    strm                    image handle reference
  @param[in]    bup_catalog             catalog reference

  @return       status
    @retval     BSTREAM_OK              ok
    @retval     != BSTREAM_OK           error
*/

static enum enum_bstream_ret_codes
read_and_print_table_data(struct st_stream *strm,
                          struct st_backup_catalog *bup_catalog)
{
  enum enum_bstream_ret_codes   brc;
  ulonglong                     chunk_cnt= 0;
  DBUG_ENTER("read_and_print_table_data");
  DBUG_ASSERT(strm);
  DBUG_ASSERT(bup_catalog);

  if (opt_mysqlbackup_data_chunks)
  {
    printf("\n");
    printf("Data chunks:\n");
  }

  /*
    Even if data chunks are not requested, we have to read them up, to
    get at the summary. Also we accumulate table sizes.
  */
  for (;;)
  {
    struct st_bstream_data_chunk data_chunk;

    brc= backup_read_snapshot(strm, bup_catalog, &data_chunk);
    if (brc != BSTREAM_OK)
    {
      /* This is the normal path to end this loop. */
      break;
    }

    chunk_cnt++;

    print_table_data(bup_catalog, &data_chunk, chunk_cnt);
  }

  /*
    Reading of snapshots must end with BSTREAM_EOC.
  */
  if (brc != BSTREAM_EOC)
    goto end;
  brc= BSTREAM_OK;

  /*
    Print table data totals.
  */
  print_table_totals(bup_catalog);

 end:
  DBUG_RETURN(brc);
}


/*
  =============
  Main program.
  =============
*/

/**
  Main

  @param[in]    argc            command line argument count
  @param[in]    argv            command line argument vector

  @return       status
    @retval     0               ok
    @retval     != 0            error

  Usage of local variables:
    bup_catalog         pointer to the catalog data structure
    strm                pointer to the backup stream reading structure
    brc                 backup stream functions return code
    status              return status of program
*/

int
main(int argc, char **argv)
{
  struct st_backup_catalog      *bup_catalog= NULL;
  struct st_stream              *strm= NULL;
  enum enum_bstream_ret_codes   brc;
  int                           status= 1; /* Assume error. */

  /*
    Client setup.
  */
  MY_INIT(argv[0]);
  DBUG_ENTER("main");
  if (init_client(&argc, &argv))
    goto end;

  /*
    Allocate catalog.
  */
  bup_catalog= backup_catalog_allocate();
  if (!bup_catalog)
    goto end;

  /*
    Open backup image stream and read header.
  */
  strm= backup_image_open(argv[0], bup_catalog);
  if (!strm)
    goto end;

  /*
    Print backup image header.
  */
  print_header(bup_catalog);

  /*
    The summary section may be inside the image header or at image end.
  */
  if (bup_catalog->cat_header.flags & BSTREAM_FLAG_INLINE_SUMMARY)
  {
    /* purecov: begin inspected */
    brc= read_and_print_summary(strm, bup_catalog);
    if (brc != BSTREAM_OK)
      goto end;

    /* Summary is printed already, no need to read it any more. */
    need_summary= FALSE;
    /* purecov: end */
  }

  /*
    We want to save image reading time if part of the information is not
    wanted. Since we read the image sequentially, we can skip a section
    only if it - and all sections behind it - are not needed.

    The need_* variables are initialized from the command line options.
    Escalate section requirement from end of image to front.
  */
  if (need_summary)
    need_tabledata= TRUE;
  if (need_tabledata)
    need_metadata= TRUE;
  if (need_metadata)
    need_catalog= TRUE;

  /*
    Read catalog.
  */
  if (need_catalog)
  {
    /* This can take some time. So flush what we have so far. */
    fflush(stdout);
    brc= backup_read_catalog(strm, bup_catalog);
    if (brc != BSTREAM_OK)
      goto end;
  }

  /*
    The first two character sets are special. They are printed with the
    header, but are available only after the catalog is read. To make
    the output somewhat user-friendly, we display them before the
    snapshots section.
  */
  print_special_charsets(bup_catalog);

  /*
    Snapshot information. We include a list of tables per snapshot,
    which is available only after the catalog is read. But we print this
    after the header and before the catalog items.
  */
  print_snapshots(bup_catalog);

  /*
    Print catalog summary.
  */
  print_catalog_summary(bup_catalog);

  /*
    Print catalog details.
  */
  print_catalog_details(bup_catalog);

  /*
    Read meta data.
  */
  if (need_metadata)
  {
    /* This can take some time. So flush what we have so far. */
    fflush(stdout);
    brc= backup_read_metadata(strm, bup_catalog);
    if (brc != BSTREAM_OK)
      goto end;
  }

  /*
    Search and print backup objects.
  */
  if (opt_mysqlbackup_search)
  {
    search_objects(bup_catalog, search_database_name, search_object_name);
    /* Success. */
    status= 0;
    goto end;
  }

  /*
    Print meta data.
  */
  print_metadata(bup_catalog);

  /*
    Read and print table data.
  */
  if (need_tabledata)
  {
    /* This can take some time. So flush what we have so far. */
    fflush(stdout);
    brc= read_and_print_table_data(strm, bup_catalog);
    if (brc != BSTREAM_OK)
      goto end;
  }

  /*
    The summary section may be inside the image header or at image end.
  */
  if (need_summary)
  {
    brc= read_and_print_summary(strm, bup_catalog);
    if (brc != BSTREAM_OK)
      goto end;
  }

  /* Success. */
  status= 0;

 end:

  /* Flush stdout to get output in right order in case of error messages. */
  fflush(stdout);

  /* Close stream. */
  if (strm)
  {
    brc= backup_image_close(strm);
    if (brc != BSTREAM_OK)
      status= 1;
  }

  /* Free catalog. */
  if (bup_catalog)
    backup_catalog_free(bup_catalog);

  /* De-initialize client. */
  cleanup_client();

  /* We cannot free DBUG, it is used in global destructors after exit(). */
  my_end(MY_DONT_FREE_DBUG);

  DBUG_RETURN(status);
}

