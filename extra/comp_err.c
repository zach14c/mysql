/* Copyright 2004-2008 MySQL AB, 2009 Sun Microsystems, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

/*
  Written by Anjuta Widenius
*/


/* ************************************************************************ *\
 *
 *  Creates one include file and multiple language-error message files
 *  from one multi-language text file.
 *
 *  Note that this source file is *not* to include "config.h" or any
 *  other header file part of this release (except possibly other
 *  header files that doesn't depend on "config.h"). The reason is
 *  that this source needs to be completely "stand alone", to support
 *  cross compilation and/or a "make dist" that doesn't have to build
 *  libraries like "libmysys" and "libmystrings".
 *
 * The only library dependency, beside normal system libraries, is zlib.
 *
 * The "*.sys" binary file format is (header is 2 byte aligned)
 *
 *    Byte Size Value Comment
 *      0  1    254   Part of magic
 *      1  1    254   Part of magic
 *      2  1      2   Part of magic
 *      3  1      1   Part of magic
 *      4  1      N   Input file count, max 10 (currently always 1)
 *      5  1      0   Not defined (always zero)
 *      6  2      2   Length    (little endian unsigned short)
 *      8  2      2   Row count (little endian unsigned short)
 *   * 10  2      2   Array of 'infile_count' (blocks ?!)
 *     12  8      0   Not defined (always zeros)
 *     30  1      N   Character set index
 *     31  1      0   Not defined (always zero)
 *   * 32  2      2   Array of file positions where msg starts
 *
 * The rest of the file contains the '\0' terminated error strings.
 *
\* ************************************************************************ */

/* To get stat() and mkdir() */
#include <sys/types.h>
#include <sys/stat.h>
#ifndef _WIN32
#include <unistd.h>
#endif

#include <errno.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <zlib.h>                               /* Declaring crc32() */


#define MY_PROGNAME "comp_err"
#define MY_VERSION  "2.1"

#define FN_REFLEN 512
#define MAX_ROWS  1000
#define HEADER_LENGTH 32       /* Length of header in errmsg.sys */
#define MSG_ALLOC_INCREMENT 16 /* Define chunks of this many message structs at a time */
#define DEFAULT_CHARSET_DIR "../sql/share/charsets"
#define ER_PREFIX "ER_"
#define WARN_PREFIX "WARN_"
static char *OUTFILE= (char*) "errmsg.sys";
static char *HEADERFILE= (char*) "mysqld_error.h";
static char *NAMEFILE= (char*) "mysqld_ername.h";
static char *STATEFILE= (char*) "sql_state.h";
static char *TXTFILE= (char*) "../sql/share/errmsg.txt";
static char *DATADIRECTORY= (char*) "../sql/share/";
static char *charsets_dir= DEFAULT_CHARSET_DIR;
static int debug= 0;

#define string_eq(_STR1, _STR2) (strcmp(_STR1, _STR2) == 0)
#define is_prefix(_STR, _PREFIX) (0 == strncmp(_STR, _PREFIX, strlen(_PREFIX)))
#define int2store(T,A)       do { unsigned int def_temp= (unsigned int) (A) ;\
                                  *((unsigned char*) (T))=  (unsigned char)(def_temp); \
                                   *((unsigned char*) (T)+1)=(unsigned char)((def_temp >> 8)); \
                             } while(0)


#ifdef _WIN32
#  define PATH_SEP_STRING "\\"
#  define MKDIR12(DIR, MODE) mkdir((DIR))
#else
#  define PATH_SEP_STRING "/"
#  define MKDIR12(DIR, MODE) mkdir((DIR),(MODE))
#endif

# define DBUG_PRINT(_TYPE, _FORMAT) do {if (debug) printf _FORMAT ;} while (0)

/*
  This is the mapping between character set names and the default
  character set id. This is a hard-coded duplicate of the list of
  character sets in sql/share/charsets/Index.xml. All character sets
  with error messages need to be listed.

  The numeric id for a character set never change so it is safe to
  hard code them here. You have to extend this table if a new language
  is added to the error messages, that uses a new charater set not in
  this list. But you will notice, this utility will stop and complain.
*/

static struct {
  int id;
  const char *name;
} cs_default_id_and_name_mapping[] =
{
  { 1,  "big5"},
  { 3,  "dec8"},
  { 4,  "cp850"},
  { 6,  "hp8"},
  { 7,  "koi8r"},
  { 8,  "latin1"},
  { 9,  "latin2"},
  {10,  "swe7"},
  {11,  "ascii"},
  {12,  "ujis"},
  {13,  "sjis"},
  {16,  "hebrew"},
  {18,  "tis620"},
  {19,  "euckr"},
  {22,  "koi8u"},
  {24,  "gb2312"},
  {25,  "greek"},
  {26,  "cp1250"},
  {28,  "gbk"},
  {30,  "latin5"},
  {32,  "armscii8"},
  {33,  "utf8"},
  {35,  "ucs2"},
  {36,  "cp866"},
  {37,  "keybcs2"},
  {38,  "macce"},
  {39,  "macroman"},
  {40,  "cp852"},
  {41,  "latin7"},
  {51,  "cp1251"},
  {57,  "cp1256"},
  {59,  "cp1257"},
  {63,  "binary"},
  {92,  "geostd8"},
  {95,  "cp932"},
  {97,  "eucjpms"},
  {0, ""}                                       /* Sentinel */
};

/* Header for errmsg.sys files */
unsigned char file_head_magic[]= { 254, 254, 2, 1 };
/* Store positions to each error message row to store in errmsg.sys header */
unsigned int file_pos[MAX_ROWS];

const char *empty_string= "";			/* For empty states */
/*
  Default values for command line options. See getopt structure for definitions
  for these.
*/

const char *default_language= "eng";
int er_offset= 1000;
int info_flag= 0;

/* Storage of one error message row (for one language) */

struct message
{
  char *lang_short_name;
  char *text;
};


/* Storage for languages and charsets (from start of error text file) */

struct languages
{
  char *lang_long_name;				/* full name of the language */
  char *lang_short_name;			/* abbreviation of the lang. */
  char *charset;				/* Character set name */
  struct languages *next_lang;			/* Pointer to next language */
};

/* Name, code and  texts (for all lang) for one error message */

struct errors
{
  const char *er_name;			/* Name of the error (ER_HASHCK) */
  int d_code;                           /* Error code number */
  const char *sql_code1;		/* sql state */
  const char *sql_code2;		/* ODBC state */
  struct errors *next_error;            /* Pointer to next error */
  struct message *msg;
  unsigned int msg_count;
  unsigned int msg_alloc_count;         /* Message count allocated */
};

static void init_messages(struct errors *errors);
static void delete_messages(struct errors *errors);
static int insert_message(struct errors *errors, struct message *message);
static char *dup_string(const char *str, size_t len);
static struct languages *parse_charset_string(char *str);
static struct errors *parse_error_string(char *ptr, int er_count);
static struct message *parse_message_string(struct message *new_message,
					    char *str);
static struct message *find_message(struct errors *err, const char *lang,
                                    int no_default);
static int check_message_format(struct errors *err,
                                const char* mess);
static int parse_input_file(const char *file_name, struct errors **top_error,
			    struct languages **top_language);
static int get_options(int argc, char **argv);
static void print_version(void);
static void usage(void);
static char *parse_text_line(char *pos);
static int copy_rows(FILE * to, char *row, int row_nr, long start_pos);
static char *parse_default_language(char *str);
static unsigned int parse_error_offset(char *str);

static char *skip_delimiters(char *str);
static char *get_word(char **str);
static char *find_end_of_word(char *str);
static void clean_up(struct languages *lang_head, struct errors *error_head);
static int create_header_files(struct errors *error_head);
static int create_sys_files(struct languages *lang_head,
			    struct errors *error_head, unsigned int row_count);
static char *strmov(char *dst, const char *src);


int main(int argc, char *argv[])
{
  {
    unsigned int row_count;
    struct errors *error_head;
    struct languages *lang_head;

    if (get_options(argc, argv))
      return(1);
    if (!(row_count= parse_input_file(TXTFILE, &error_head, &lang_head)))
    {
      fprintf(stderr, "Failed to parse input file %s\n", TXTFILE);
      return(1);
    }
    if (lang_head == NULL || error_head == NULL)
    {
      fprintf(stderr, "Failed to parse input file %s\n", TXTFILE);
      return(1);
    }

    if (create_header_files(error_head))
    {
      fprintf(stderr, "Failed to create header files\n");
      return(1);
    }
    if (create_sys_files(lang_head, error_head, row_count))
    {
      fprintf(stderr, "Failed to create sys files\n");
      return(1);
    }
    clean_up(lang_head, error_head);
    return 0;
  }
}


static void print_escaped_string(FILE *f, const char *str)
{
  const char *tmp = str;

  while (tmp[0] != 0)
  {
    switch (tmp[0])
    {
      case '\\': fprintf(f, "\\\\"); break;
      case '\'': fprintf(f, "\\\'"); break;
      case '\"': fprintf(f, "\\\""); break;
      case '\n': fprintf(f, "\\n"); break;
      case '\r': fprintf(f, "\\r"); break;
      default: fprintf(f, "%c", tmp[0]);
    }
    tmp++;
  }
}


static int create_header_files(struct errors *error_head)
{
  unsigned int er_last = 0;             /* Initialize to make lint happy */
  FILE *er_definef, *sql_statef, *er_namef;
  struct errors *tmp_error;
  struct message *er_msg;
  const char *er_text;

  if (!(er_definef= fopen(HEADERFILE, "w")))
  {
    fprintf(stderr, "ERROR: can't open \"%s\" for writing\n", HEADERFILE);
    return(1);
  }
  if (!(sql_statef= fopen(STATEFILE, "w")))
  {
    fprintf(stderr, "ERROR: can't open \"%s\" for writing\n", STATEFILE);
    fclose(er_definef);
    return(1);
  }
  if (!(er_namef= fopen(NAMEFILE, "w")))
  {
    fprintf(stderr, "ERROR: can't open \"%s\" for writing\n", NAMEFILE);
    fclose(er_definef);
    fclose(sql_statef);
    return(1);
  }

  fprintf(er_definef, "/* Autogenerated file, please don't edit */\n\n");
  fprintf(sql_statef, "/* Autogenerated file, please don't edit */\n\n");
  fprintf(er_namef, "/* Autogenerated file, please don't edit */\n\n");

  fprintf(er_definef, "#define ER_ERROR_FIRST %d\n", error_head->d_code);

  for (tmp_error= error_head; tmp_error; tmp_error= tmp_error->next_error)
  {
    /*
       generating mysqld_error.h
       fprintf() will automatically add \r on windows
    */
    fprintf(er_definef, "#define %s %d\n", tmp_error->er_name,
	    tmp_error->d_code);
    er_last= tmp_error->d_code;

    /* generating sql_state.h file */
    if (tmp_error->sql_code1[0] || tmp_error->sql_code2[0])
      fprintf(sql_statef,
	      "{ %-40s,\"%s\", \"%s\" },\n", tmp_error->er_name,
	      tmp_error->sql_code1, tmp_error->sql_code2);
    /*generating er_name file */
    er_msg= find_message(tmp_error, default_language, 0);
    er_text = (er_msg ? er_msg->text : "");
    fprintf(er_namef, "{ \"%s\", %d, \"", tmp_error->er_name,
	    tmp_error->d_code);
    print_escaped_string(er_namef, er_text);
    fprintf(er_namef, "\" },\n");

  }
  /* finishing off with mysqld_error.h */
  fprintf(er_definef, "#define ER_ERROR_LAST %d\n", er_last);
  fclose(er_definef);
  fclose(sql_statef);
  fclose(er_namef);
  return(0);
}

static unsigned int get_charset_number(const char *cs_name)
{
  int i;
  for (i = 0; cs_default_id_and_name_mapping[i].id > 0; i++) {
    if (string_eq(cs_name, cs_default_id_and_name_mapping[i].name))
      return cs_default_id_and_name_mapping[i].id;
  }
  return 0;
}

static int create_sys_files(struct languages *lang_head,
			    struct errors *error_head, unsigned int row_count)
{
  FILE *to;
  unsigned int csnum= 0, length, i, row_nr;
  unsigned char head[HEADER_LENGTH];
  char outfile[FN_REFLEN], *outfile_end;
  long start_pos;
  struct message *tmp;
  struct languages *tmp_lang;
  struct errors *tmp_error;

  struct stat stat_info;

  /*
     going over all languages and assembling corresponding error messages
  */
  for (tmp_lang= lang_head; tmp_lang; tmp_lang= tmp_lang->next_lang)
  {
    /* setting charset name */
    if (!(csnum= get_charset_number(tmp_lang->charset)))
    {
      fprintf(stderr, "Unknown charset '%s' in '%s'\n", tmp_lang->charset,
	      TXTFILE);
      return(1);
    }

    if (debug) {
      fprintf(stderr, "lang_long_name  : %s\n", tmp_lang->lang_long_name);
      fprintf(stderr, "lang_short_name : %s\n", tmp_lang->lang_short_name);
      fprintf(stderr, "charset name    : %s\n", tmp_lang->charset);
      fprintf(stderr, "charset id      : %d\n\n", csnum);
    }

    strcpy(outfile, DATADIRECTORY);
    strcat(outfile, tmp_lang->lang_long_name);
    outfile_end= strchr(outfile, '\0');

    if (stat(outfile, &stat_info) != 0)
    {
      if (MKDIR12(outfile, 0777) != 0)
      {
        fprintf(stderr, "Can't create output directory for %s\n", 
                outfile);
        return(1);
      }
    }

    strcpy(outfile_end, PATH_SEP_STRING);
    strcat(outfile_end, OUTFILE);

    if (!(to= fopen(outfile, "wb"))) {
      fprintf(stderr, "ERROR: can't open \"%s\" for writing\n", outfile);
      return(1);
    }

    /* 2 is for 2 bytes to store row position / error message */
    start_pos= (long) (HEADER_LENGTH + row_count * 2);
    if (fseek(to, start_pos, SEEK_SET) != 0) {
      fprintf(stderr, "ERROR: can't seek file \"%s\"\n", outfile);
      goto err;
    }

    row_nr= 0;
    for (tmp_error= error_head; tmp_error; tmp_error= tmp_error->next_error)
    {
      /* dealing with messages */
      tmp= find_message(tmp_error, tmp_lang->lang_short_name, 0);

      if (!tmp)
      {
	fprintf(stderr,
		"Did not find message for %s neither in %s nor in default "
		"language\n", tmp_error->er_name, tmp_lang->lang_short_name);
	goto err;
      }
      if (copy_rows(to, tmp->text, row_nr, start_pos))
      {
	fprintf(stderr, "Failed to copy rows to %s\n", outfile);
	goto err;
      }
      row_nr++;
    }

    /* continue with header of the errmsg.sys file */
    length= ftell(to) - HEADER_LENGTH - row_count * 2;
    memset(head, 0, HEADER_LENGTH);
    head[0]= file_head_magic[0];
    head[1]= file_head_magic[1];
    head[2]= file_head_magic[2];
    head[3]= file_head_magic[3];
    head[4]= 1;
    /* head[5] Undefined, always zero */
    int2store(&head[6], length);
    int2store(&head[8], row_count);
    head[30]= csnum;
    /* head[31] Undefined, always zero */

    if (fseek(to, 0l, SEEK_SET) != 0) {
      fprintf(stderr, "ERROR: can't seek file \"%s\"\n", outfile);
      goto err;
    }
    if (fwrite(head, HEADER_LENGTH, 1, to) != 1) {
      fprintf(stderr, "ERROR: can't write %d bytes to \"%s\"\n",
              HEADER_LENGTH, outfile);
      goto err;
    }

    for (i= 0; i < row_count; i++)
    {
      unsigned char buf[2];
      int2store(buf, file_pos[i]);
      if (fwrite(buf, 2, 1, to) != 1) {
	fprintf(stderr, "ERROR: can't write 2 bytes to \"%s\"\n", outfile);
	goto err;
      }
    }
    fclose(to);
  }
  return(0);

err:
  fclose(to);
  return(1);
}


static void clean_up(struct languages *lang_head, struct errors *error_head)
{
  struct languages *tmp_lang, *next_language;
  struct errors *tmp_error, *next_error;
  unsigned int count, i;

  free((unsigned char*) default_language);

  for (tmp_lang= lang_head; tmp_lang; tmp_lang= next_language)
  {
    next_language= tmp_lang->next_lang;
    free(tmp_lang->lang_short_name);
    free(tmp_lang->lang_long_name);
    free(tmp_lang->charset);
    free((unsigned char*) tmp_lang);
  }

  for (tmp_error= error_head; tmp_error; tmp_error= next_error)
  {
    next_error= tmp_error->next_error;
    count= tmp_error->msg_count;
    for (i= 0; i < count; i++)
    {
      struct message *tmp= &tmp_error->msg[i];
      free((unsigned char*) tmp->lang_short_name);
      free((unsigned char*) tmp->text);
    }

    delete_messages(tmp_error);
    if (tmp_error->sql_code1[0])
      free((unsigned char*) tmp_error->sql_code1);
    if (tmp_error->sql_code2[0])
      free((unsigned char*) tmp_error->sql_code2);
    free((unsigned char*) tmp_error->er_name);
    free((unsigned char*) tmp_error);
  }
}


static int parse_input_file(const char *file_name, struct errors **top_error,
			    struct languages **top_lang)
{
  FILE *file;
  char *str, buff[1000];
  struct errors *current_error= 0, **tail_error= top_error;
  struct message current_message;
  int rcount= 0;

  *top_error= 0;
  *top_lang= 0;
  if (!(file= fopen(file_name, "r"))) {
    fprintf(stderr, "ERROR: can't open \"%s\" for reading\n", file_name);
    return(0);
  }

  while ((str= fgets(buff, sizeof(buff), file)))
  {
    if (is_prefix(str, "language"))
    {
      if (!(*top_lang= parse_charset_string(str)))
      {
	fprintf(stderr, "Failed to parse the charset string!\n");
	return(0);
      }
      continue;
    }
    if (is_prefix(str, "start-error-number"))
    {
      if (!(er_offset= parse_error_offset(str)))
      {
	fprintf(stderr, "Failed to parse the error offset string!\n");
	return(0);
      }
      continue;
    }
    if (is_prefix(str, "default-language"))
    {
      if (!(default_language= parse_default_language(str)))
      {
	DBUG_PRINT("info", ("default_slang: %s", default_language));
	fprintf(stderr,
		"Failed to parse the default language line. Aborting\n");
	return(0);
      }
      continue;
    }

    if (*str == '\t' || *str == ' ')
    {
      /* New error message in another language for previous error */
      if (!current_error)
      {
	fprintf(stderr, "Error in the input file format\n");
	return(0);
      }
      if (!parse_message_string(&current_message, str))
      {
	fprintf(stderr, "Failed to parse message string for error '%s'",
		current_error->er_name);
	return(0);
      }
      if (find_message(current_error, current_message.lang_short_name, 1))
      {
	fprintf(stderr, "Duplicate message string for error '%s'"
                        " in language '%s'\n",
		current_error->er_name, current_message.lang_short_name);
	return(0);
      }
      if (check_message_format(current_error, current_message.text))
      {
	fprintf(stderr, "Wrong formatspecifier of error message string"
                        " for error '%s' in language '%s'\n",
		current_error->er_name, current_message.lang_short_name);
	return(0);
      }
      if (insert_message(current_error, &current_message))
	return(0);
      continue;
    }
    if (is_prefix(str, ER_PREFIX) || is_prefix(str, WARN_PREFIX))
    {
      if (!(current_error= parse_error_string(str, rcount)))
      {
	fprintf(stderr, "Failed to parse the error name string\n");
	return(0);
      }
      rcount++;                         /* Count number of unique errors */

      /* add error to the list */
      *tail_error= current_error;
      tail_error= &current_error->next_error;
      continue;
    }
    if (*str == '#' || *str == '\n')
      continue;					/* skip comment or empty lines */

    fprintf(stderr, "Wrong input file format. Stop!\nLine: %s\n", str);
    return(0);
  }
  *tail_error= 0;				/* Mark end of list */

  fclose(file);
  return(rcount);
}


static unsigned int parse_error_offset(char *str)
{
  char *soffset, *end;
  unsigned int ioffset;

  /* skipping the "start-error-number" keyword and spaces after it */
  str= find_end_of_word(str);
  str= skip_delimiters(str);

  if (!*str)
    return(0);     /* Unexpected EOL: No error number after the keyword */

  /* reading the error offset */
  soffset= get_word(&str);
  DBUG_PRINT("info", ("default_error_offset: %s", soffset));

  /* skipping space(s) and/or tabs after the error offset */
  str= skip_delimiters(str);
  DBUG_PRINT("info", ("str: %s", str));
  if (*str)
  {
    /* The line does not end with the error offset -> error! */
    fprintf(stderr, "The error offset line does not end with an error offset");
    return(0);
  }
  DBUG_PRINT("info", ("str: %s", str));

  end= 0;
  ioffset= (unsigned int) strtol(soffset, &end, 10);
  free((unsigned char*) soffset);
  return(ioffset);
}


/* Parsing of the default language line. e.g. "default-language eng" */

static char *parse_default_language(char *str)
{
  char *slang;

  /* skipping the "default-language" keyword */
  str= find_end_of_word(str);
  /* skipping space(s) and/or tabs after the keyword */
  str= skip_delimiters(str);
  if (!*str)
  {
    fprintf(stderr,
	    "Unexpected EOL: No short language name after the keyword\n");
    return(0);
  }

  /* reading the short language tag */
  slang= get_word(&str);
  DBUG_PRINT("info", ("default_slang: %s", slang));

  str= skip_delimiters(str);
  DBUG_PRINT("info", ("str: %s", str));
  if (*str)
  {
    fprintf(stderr,
	    "The default language line does not end with short language "
	    "name\n");
    return(0);
  }
  DBUG_PRINT("info", ("str: %s", str));
  return(slang);
}


/*
  Find the message in a particular language

  SYNOPSIS
    find_message()
    err             Error to find message for
    lang            Language of message to find
    no_default      Don't return default (English) if does not exit

  RETURN VALUE
    Returns the message structure if one is found, or NULL if not.
*/
static struct message *find_message(struct errors *err, const char *lang,
                                    int no_default)
{
  struct message *return_val= 0;
  unsigned int i, count;

  count= err->msg_count;
  for (i= 0; i < count; i++)
  {
    struct message *tmp= &err->msg[i];

    if (string_eq(tmp->lang_short_name, lang))
      return(tmp);
    if (string_eq(tmp->lang_short_name, default_language))
    {
      assert(tmp->text[0] != 0);
      return_val= tmp;
    }
  }
  return(no_default ? NULL : return_val);
}



/*
  Check message format specifiers against error message for
  previous language

  SYNOPSIS
    checksum_format_specifier()
    msg            String for which to generate checksum
                   for the format specifiers

  RETURN VALUE
    Returns the checksum for all the characters of the
    format specifiers

    Ex.
     "text '%-64.s' text part 2 %d'"
            ^^^^^^              ^^
            characters will be xored to form checksum

    NOTE:
      Does not support format specifiers with positional args
      like "%2$s" but that is not yet supported by my_vsnprintf
      either.
*/

static unsigned long checksum_format_specifier(const char* msg)
{
  unsigned long chksum= 0;
  const unsigned char* p= (const unsigned char*)msg;
  const unsigned char* start= 0;
  int num_format_specifiers= 0;
  while (*p)
  {

    if (*p == '%')
    {
      start= p+1; /* Entering format specifier */
      num_format_specifiers++;
    }
    else if (start)
    {
      switch(*p)
      {
      case 'd':
      case 'u':
      case 'x':
      case 's':
        chksum= crc32(chksum, start, p - start);
        start= 0; /* Not in format specifier anymore */
        break;

      default:
        break;
      }
    }

    p++;
  }

  if (start)
  {
    /* Still inside a format specifier after end of string */

    fprintf(stderr, "Still inside formatspecifier after end of string"
                    " in'%s'\n", msg);
    assert(start==0);
  }

  /* Add number of format specifiers to checksum as extra safeguard */
  chksum+= num_format_specifiers;

  return chksum;
}


/*
  Check message format specifiers against error message for
  previous language

  SYNOPSIS
    check_message_format()
    err             Error to check message for
    mess            Message to check

  RETURN VALUE
    Returns 0 if no previous error message or message format is ok
*/
static int check_message_format(struct errors *err,
                                const char* mess)
{
  struct message *first;

  /*  Get first message(if any) */
  if (err->msg_count == 0)
    return(0); /* No previous message to compare against */

  first= &err->msg[0];
  assert(first != NULL);

  if (checksum_format_specifier(first->text) !=
      checksum_format_specifier(mess))
  {
    /* Check sum of format specifiers failed, they should be equal */
    return(1);
  }
  return(0);
}


/*
  Skips spaces and or tabs till the beginning of the next word
  Returns pointer to the beginning of the first character of the word
*/

static char *skip_delimiters(char *str)
{
  for (;
       *str == ' ' || *str == ',' || *str == '\t' || *str == '\r' ||
       *str == '\n' || *str == '='; str++)
    ;
  return(str);
}


/*
  Skips all characters till meets with space, or tab, or EOL
*/

static char *find_end_of_word(char *str)
{
  for (;
       *str != ' ' && *str != '\t' && *str != '\n' && *str != '\r' && *str &&
       *str != ',' && *str != ';' && *str != '='; str++)
    ;
  return(str);
}


/* Read the word starting from *str */

static char *get_word(char **str)
{
  char *start= *str;

  *str= find_end_of_word(start);
  return(dup_string(start, (unsigned int) (*str - start)));
}


/*
  Parsing the string with short_lang - message text. Code - to
  remember to which error does the text belong
*/

static struct message *parse_message_string(struct message *new_message,
					    char *str)
{
  char *start;

  DBUG_PRINT("enter", ("str: %s", str));

  /*skip space(s) and/or tabs in the beginning */
  while (*str == ' ' || *str == '\t' || *str == '\n')
    str++;

  if (!*str)
  {
    /* It was not a message line, but an empty line. */
    DBUG_PRINT("info", ("str: %s", str));
    return(0);
  }

  /* reading the short lang */
  start= str;
  while (*str != ' ' && *str != '\t' && *str)
    str++;
  new_message->lang_short_name=
    dup_string(start, (unsigned int) (str - start));
  DBUG_PRINT("info", ("msg_slang: %s", new_message->lang_short_name));

  /*skip space(s) and/or tabs after the lang */
  while (*str == ' ' || *str == '\t' || *str == '\n')
    str++;

  if (*str != '"')
  {
    fprintf(stderr, "Unexpected EOL");
    DBUG_PRINT("info", ("str: %s", str));
    return(0);
  }

  /* reading the text */
  start= str + 1;
  str= parse_text_line(start);

  new_message->text= dup_string(start, (unsigned int) (str - start));
  DBUG_PRINT("info", ("msg_text: %s", new_message->text));

  return(new_message);
}


/*
  Parsing the string with error name and codes; returns the pointer to
  the errors struct
*/

static struct errors *parse_error_string(char *str, int er_count)
{
  struct errors *new_error;
  char *start;
  DBUG_PRINT("enter", ("str: %s", str));

  /* create a new element */
  if (!(new_error= (struct errors *) malloc(sizeof(*new_error)))) {
    fprintf(stderr, "ERROR: can't allocate \"%d\" bytes\n", (int)sizeof(*new_error));
    exit(1);
  }

  init_messages(new_error);

  /* getting the error name */
  start= str;
  str= skip_delimiters(str);

  if (!(new_error->er_name= get_word(&str)))
    return(0);				/* OOM: Fatal error */
  DBUG_PRINT("info", ("er_name: %s", new_error->er_name));

  str= skip_delimiters(str);

  /* getting the code1 */

  new_error->d_code= er_offset + er_count;
  DBUG_PRINT("info", ("d_code: %d", new_error->d_code));

  str= skip_delimiters(str);

  /* if we reached EOL => no more codes, but this can happen */
  if (!*str)
  {
    new_error->sql_code1= empty_string;
    new_error->sql_code2= empty_string;
    DBUG_PRINT("info", ("str: %s", str));
    return(new_error);
  }

  /* getting the sql_code 1 */

  if (!(new_error->sql_code1= get_word(&str)))
    return(0);				/* OOM: Fatal error */
  DBUG_PRINT("info", ("sql_code1: %s", new_error->sql_code1));

  str= skip_delimiters(str);

  /* if we reached EOL => no more codes, but this can happen */
  if (!*str)
  {
    new_error->sql_code2= empty_string;
    DBUG_PRINT("info", ("str: %s", str));
    return(new_error);
  }

  /* getting the sql_code 2 */
  if (!(new_error->sql_code2= get_word(&str)))
    return(0);				/* OOM: Fatal error */
  DBUG_PRINT("info", ("sql_code2: %s", new_error->sql_code2));

  str= skip_delimiters(str);
  if (*str)
  {
    fprintf(stderr, "The error line did not end with sql/odbc code!");
    return(0);
  }

  return(new_error);
}


/* 
  Parsing the string with full lang name/short lang name/charset;
  returns pointer to the language structure
*/

static struct languages *parse_charset_string(char *str)
{
  struct languages *head=0, *new_lang;
  DBUG_PRINT("enter", ("str: %s", str));

  /* skip over keyword */
  str= find_end_of_word(str);
  if (!*str)
  {
    /* unexpected EOL */
    DBUG_PRINT("info", ("str: %s", str));
    return(0);
  }

  str= skip_delimiters(str);
  if (!(*str != ';' && *str))
    return(0);

  do
  {
    /*creating new element of the linked list */
    if (!(new_lang= (struct languages *) malloc(sizeof(*new_lang)))) {
      fprintf(stderr, "ERROR: can't allocate \"%d\" bytes\n", (int)sizeof(*new_lang));
      exit(1);
    }
    new_lang->next_lang= head;
    head= new_lang;

    /* get the full language name */

    if (!(new_lang->lang_long_name= get_word(&str)))
      return(0);				/* OOM: Fatal error */

    DBUG_PRINT("info", ("long_name: %s", new_lang->lang_long_name));

    /* getting the short name for language */
    str= skip_delimiters(str);
    if (!*str)
      return(0);				/* Error: No space or tab */

    if (!(new_lang->lang_short_name= get_word(&str)))
      return(0);				/* OOM: Fatal error */
    DBUG_PRINT("info", ("short_name: %s", new_lang->lang_short_name));

    /* getting the charset name */
    str= skip_delimiters(str);
    if (!(new_lang->charset= get_word(&str)))
      return(0);				/* Fatal error */
    DBUG_PRINT("info", ("charset: %s", new_lang->charset));

    /* skipping space, tab or "," */
    str= skip_delimiters(str);
  }
  while (*str != ';' && *str);

  DBUG_PRINT("info", ("long name: %s", new_lang->lang_long_name));
  return(head);
}


/* Read options */

static void print_version(void)
{
  printf("%s  (Compile errormessage)  Ver %s\n", MY_PROGNAME, MY_VERSION);
  return;
}


static void usage(void)
{
  print_version();
  printf("This software comes with ABSOLUTELY NO WARRANTY. This is free software,\n"
         "and you are welcome to modify and redistribute it under the GPL license.\n\n");
  printf("Usage:\n\n"
         "  --debug            Enable debug printouts\n"
         "  --debug-info       Print some debug info at exit\n"
         "  --help             Displays this help and exits\n"
         "  --version          Prints version\n"
         "  --charset=DIR      Charset dir\n"
         "  --in_file=PATH     Input file\n"
         "  --out-dir=DIR      Output base directory\n"
         "  --out_file=NAME    Output filename (errmsg.sys)\n"
         "  --header_file=PATH mysqld_error.h file\n"
         "  --name_file=PATH   mysqld_ername.h file\n"
         "  --state_file=PATH  sql_state.h file\n");
  return;
}


static int get_options(int argc, char **argv)
{
  int i;

  for (i = 1; i < argc; i++) {
    const char *argp= argv[i];
    if (string_eq(argp, "--debug")) {
      debug= 1;
    } else if  (string_eq(argp, "--debug-info")) {
      info_flag= 1;                             /* Does nothing right now */
     } else if  (string_eq(argp, "--help")) {
      usage();
      exit(0);
    } else if  (string_eq(argp, "--version")) {
      print_version();
      exit(0);
    } else {
      /* The rest is --<opt>=<value> */
      char *val= strchr(argp, '=');
      if (!val) {
        fprintf(stderr,
                "ERROR: Invalid option '%s', or missing required =ARGUMENT\n",
                argp);
        return(1);
      }
      *val= '\0';                             /* Terminate option name */
      val++;                                    /* Pointer to value */
      if  (string_eq(argp, "--charset")) {
        charsets_dir= val;
      } else if  (string_eq(argp, "--in_file")) {
        TXTFILE= val;
      } else if  (string_eq(argp, "--out-dir")) {
        DATADIRECTORY= val;
      } else if  (string_eq(argp, "--out_file")) {
        OUTFILE= val;
      } else if  (string_eq(argp, "--header_file")) {
        HEADERFILE= val;
      } else if  (string_eq(argp, "--name_file")) {
        NAMEFILE= val;
      } else if  (string_eq(argp, "--state_file")) {
        STATEFILE= val;
      } else {
        fprintf(stderr, "ERROR: unknown option '%s'\n", argp);
        return(1);
      }
    }
  }

  return(0);
}


/*
  Read rows and remember them until row that start with char Converts
  row as a C-compiler would convert a textstring
*/

static char *parse_text_line(char *pos)
{
  int i, nr;
  char *row= pos;

  /* FIXME error if ending with \ ? */
  while (*pos)
  {
    if (*pos == '\\')
    {
      switch (*++pos) {
      case 'n':
	pos[0]= '\n';                           /* Prepare for move */
	/* FALLTHROUGH */
      case '\\':
      case '"':
	pos[-1]= pos[0];
	strmov(pos, pos + 1);
	break;
      default:
	if (*pos >= '0' && *pos < '8')
	{
	  nr= 0;
	  for (i= 0; i < 3 && (*pos >= '0' && *pos < '8'); i++)
	    nr= nr * 8 + (*(pos++) - '0');
	  pos -= i;
	  pos[-1]= nr;
	  strmov(pos, pos + 1);
	}
	else if (*pos)                          /* FIXME error really?! */
	  strmov(pos - 1, pos);                 /* Remove '\' */
      }
    }
    else
      pos++;
  }
  while (pos > row + 1 && *pos != '"')
    pos--;
  *pos= 0;
  return(pos);
}


/* Copy rows from memory to file and remember position */

static int copy_rows(FILE *to, char *row, int row_nr, long start_pos)
{
  file_pos[row_nr]= (int) (ftell(to) - start_pos);
  if (fputs(row, to) == EOF || fputc('\0', to) == EOF)
  {
    fprintf(stderr, "Can't write to outputfile\n");
    return(1);
  }

  return(0);
}

/*
  Initialize the message structure part of struct errors
*/

static void init_messages(struct errors *errors)
{
  errors->msg_count= 0;
  errors->msg_alloc_count= MSG_ALLOC_INCREMENT;
  errors->msg= (struct message *) malloc(errors->msg_alloc_count *
                                         sizeof(struct message));
  if (!errors->msg) {
    fprintf(stderr, "ERROR: can't allocate messages of %ld bytes\n",
            errors->msg_alloc_count * sizeof(struct message));
    exit(1);
  }
}

/*
  Clear the dynamic message structure
*/

static void delete_messages(struct errors *errors)
{
  errors->msg_count= 0;
  errors->msg_alloc_count= 0;
  free(errors->msg);
  errors->msg= NULL;
}

/*
  Insert a message structure into a dynamic array, at the end
*/

static int insert_message(struct errors *errors, struct message *message)
{
  if (errors->msg_count == errors->msg_alloc_count) {
    void *old_mem= (void *)errors->msg;
    errors->msg_alloc_count+= MSG_ALLOC_INCREMENT; /* Increase size */
    errors->msg= (struct message *) realloc(old_mem,
                                            errors->msg_alloc_count *
                                            sizeof(struct message));
    if (!errors->msg) {
      fprintf(stderr, "ERROR: can't allocate messages of %ld bytes\n",
              errors->msg_alloc_count * sizeof(struct message));
      free(old_mem);
      return(1);
    }
  }
   
  errors->msg_count++;
  errors->msg[errors->msg_count - 1]= *message;

  return(0);
}

/*
  Function similar to GNU strndup(), duplicate a string
  but at most 'len' bytes. Terminate with a '\0'.
  Never returns NULL; will exit on error.
*/

static char *dup_string(const char *str, size_t len)
{
  size_t str_len= strlen(str);
  size_t buf_len= len > str_len ? str_len : len;   /* Minimum */
  char *buf= malloc(++buf_len);                 /* Include '\0' */
  if (!buf) {
    fprintf(stderr, "ERROR: can't allocate \"%d\" bytes\n", (int)buf_len);
    exit(1);
  }
  strncpy(buf, str, buf_len-1);
  buf[buf_len-1]= '\0';
  return buf;
}


static char *strmov(char *dst, const char *src)
{
  while ((*dst++ = *src++)) ;
  return dst-1;
}
