/* Copyright (C) 2000-2006 MySQL AB

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

/**
  @file
  Utility to display and apply a MyISAM logical or physical log to tables.

  Prints what is in a MyISAM (logical or physical/backup) log, optionally
  applies the changes to tables (all tables or only a set specified on the
  command line). Works standalone (tables must not be modified by the
  server during this).
*/

#ifndef USE_MY_FUNC
#define USE_MY_FUNC
#endif

#include "myisamdef.h"
#include <my_tree.h>
#include <stdarg.h>
#ifdef HAVE_GETRUSAGE
#include <sys/resource.h>
#endif

#define NO_FILEPOS (ulong) ~0L

static void get_options(int *argc,char ***argv);
static my_bool matches_list_of_tables(const char *isam_file_name);

static MI_EXAMINE_LOG_PARAM mi_exl;
static char **table_names;

static uint test_info=0;

int main(int argc, char **argv)
{
  int error,i,first;
  ulong total_count,total_error,total_recover;
  MY_INIT(argv[0]);

  mi_examine_log_param_init(&mi_exl);
  mi_exl.log_filename= myisam_logical_log_filename; /* the default */
  get_options(&argc,&argv);
  if (argv[0]) /* some table names passed on command line */
  {
    table_names= argv;
    mi_exl.table_selection_hook= matches_list_of_tables;
  }

  /* Number of MyISAM files we can have open at one time */
  mi_exl.max_files= (my_set_max_open_files(max(mi_exl.max_files,8))-6)/2;

  /*
    Program must work in all conditions: support symbolic links.
    It should not be a security risk.
  */
#ifdef USE_SYMDIR
  my_use_symdir= 1;
#endif

  if (mi_exl.update)
    printf("Trying to %s MyISAM files according to log '%s'\n",
	   (mi_exl.recover ? "recover" : "update"),mi_exl.log_filename);

  error= mi_examine_log(&mi_exl);

  if (mi_exl.update && ! error)
    puts("Tables updated successfully");
  total_count=total_error=total_recover=0;
  for (i=first=0 ; mi_log_command_name[i] ; i++)
  {
    if (mi_exl.com_count[i][0])
    {
      if (!first++)
      {
	if (mi_exl.verbose || mi_exl.update)
	  puts("");
	puts("Commands                         Used count    Errors"
             " Recover errors");
      }
      printf("%-20s%9ld%10ld%15ld\n", mi_log_command_name[i],
             mi_exl.com_count[i][0],
	     mi_exl.com_count[i][1],mi_exl.com_count[i][2]);
      total_count+=mi_exl.com_count[i][0];
      total_error+=mi_exl.com_count[i][1];
      total_recover+=mi_exl.com_count[i][2];
    }
  }
  if (total_count)
    printf("%-12s%9ld%10ld%17ld\n","Total",total_count,total_error,
	   total_recover);
  if (mi_exl.re_open_count)
    printf("Had to do %d re-open because of too few possibly open files\n",
	   mi_exl.re_open_count);
  (void) mi_panic(HA_PANIC_CLOSE);
  my_free_open_file_info();
  my_end(test_info ? MY_CHECK_ERROR | MY_GIVE_INFO : MY_CHECK_ERROR);
  exit(error);
  return 0;				/* No compiler warning */
} /* main */


static void get_options(register int *argc, register char ***argv)
{
  int help,version;
  const char *pos,*usage;
  char option;

  help=0;
  usage="Usage: %s [-?iruvDIV] [-c #] [-f #] [-F filepath/] [-o #] [-R file recordpos] [-w write_file] [log-filename [table ...]] \n";
  pos="";

  while (--*argc > 0 && *(pos = *(++*argv)) == '-' ) {
    while (*++pos)
    {
      version=0;
      switch((option=*pos)) {
      case '#':
	DBUG_PUSH (++pos);
	pos=" ";				/* Skip rest of arg */
	break;
      case 'c':
	if (! *++pos)
	{
	  if (!--*argc)
	    goto err;
	  else
	    pos= *(++*argv);
	}
	mi_exl.number_of_commands= (ulong) atol(pos);
	pos=" ";
	break;
      case 'u':
	mi_exl.update=1;
	break;
      case 'f':
	if (! *++pos)
	{
	  if (!--*argc)
	    goto err;
	  else
	    pos= *(++*argv);
	}
	mi_exl.max_files=(uint) atoi(pos);
	pos=" ";
	break;
      case 'i':
	test_info=1;
	break;
      case 'o':
	if (! *++pos)
	{
	  if (!--*argc)
	    goto err;
	  else
	    pos= *(++*argv);
	}
	mi_exl.start_offset=(my_off_t) strtoll(pos,NULL,10);
	pos=" ";
	break;
      case 'p':
	if (! *++pos)
	{
	  if (!--*argc)
	    goto err;
	  else
	    pos= *(++*argv);
	}
	mi_exl.prefix_remove=atoi(pos);
	break;
      case 'r':
	mi_exl.update=1;
	mi_exl.recover++;
	break;
      case 'P':
	mi_exl.opt_processes=1;
	break;
      case 'R':
	if (! *++pos)
	{
	  if (!--*argc)
	    goto err;
	  else
	    pos= *(++*argv);
	}
	mi_exl.record_pos_file=(char*) pos;
	if (!--*argc)
	  goto err;
	mi_exl.record_pos=(my_off_t) strtoll(*(++*argv),NULL,10);
	pos=" ";
	break;
      case 'v':
	mi_exl.verbose++;
	break;
      case 'w':
	if (! *++pos)
	{
	  if (!--*argc)
	    goto err;
	  else
	    pos= *(++*argv);
	}
	mi_exl.write_filename=(char*) pos;
	pos=" ";
	break;
      case 'F':
	if (! *++pos)
	{
	  if (!--*argc)
	    goto err;
	  else
	    pos= *(++*argv);
	}
	mi_exl.filepath= (char*) pos;
	pos=" ";
	break;
      case 'V':
	version=1;
	/* Fall through */
      case 'I':
      case '?':
#include <help_start.h>
	printf("%s  Ver 2.0 for %s at %s\n",my_progname,SYSTEM_TYPE,
	       MACHINE_TYPE);
	puts("By Monty, for your professional use\n");
	if (version)
	  break;
	puts("Write info about whats in a MyISAM log file.");
	printf("If no file name is given %s is used\n",mi_exl.log_filename);
	puts("");
	printf(usage,my_progname);
	puts("");
	puts("Options: -? or -I \"Info\"     -V \"version\"   -c \"do only # commands\"");
	puts("         -f \"max open files\" -F \"filepath\"  -i \"extra info\"");
	puts("         -o \"offset\"         -p # \"remove # components from path\"");
	puts("         -r \"recover\"        -R \"file recordposition\"");
	puts("         -u \"update\"         -v \"verbose\"   -w \"write file\"");
	puts("         -D \"myisam compiled with DBUG\"   -P \"processes\"");
	puts("\nOne can give a second and a third '-v' for more verbose.");
	puts("Normaly one does a update (-u).");
	puts("If a recover is done all writes and all possibly updates and deletes is done\nand errors are only counted.");
	puts("If one gives table names as arguments only these tables will be updated\n");
	help=1;
#include <help_end.h>
	break;
      default:
	printf("illegal option: \"-%c\"\n",*pos);
	break;
      }
    }
  }
  if (! *argc)
  {
    if (help)
    exit(0);
    (*argv)++;
  }
  if (*argc >= 1)
  {
    mi_exl.log_filename=(char*) pos;
    (*argc)--;
    (*argv)++;
  }
  return;
 err:
  (void) fprintf(stderr,"option \"%c\" used without or with wrong argument\n",
	       option);
  exit(1);
}


static my_bool matches_list_of_tables(const char *isam_file_name)
{
  if (table_names && table_names[0])
  {
    char **name;
    for (name= table_names ; *name ; name++)
    {
      if (!strcmp(*name, isam_file_name))
        return 1;
    }
    return 0;
  }
  return 1;
}

#include "mi_extrafunc.h"
