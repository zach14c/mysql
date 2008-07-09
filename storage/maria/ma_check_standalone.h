/* Copyright (C) 2007 MySQL AB

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
  Most standalone Maria programs (maria_chk, maria_read_log, etc, unit
  tests) use objects from MyISAM, and end up in link dependencies similar to
  this one:
  maria_chk.o needs ft_init_stopwords() => needs storage/myisam/ft_stopwords.o
  ft_stopwords.o needs ft_stopword_file => needs ft_static.o
  ft_static.o needs ft_nlq_read_next => needs ft_nlq_search.o
  ft_nlq_search.o needs mi_readinfo() => needs mi_locking.o
  mi_locking.o needs mi_state_info_write() => needs mi_open.o
  mi_open.o needs mi_ck_write => needs mi_write.o
  mi_write.o needs _mi_report_crashed() and that is a problem because
  defined only in ha_myisam.o, thus ha_myisam.o is brought in, with its
  dependencies on mysqld.o, which make linking fail.
  The solution is to declare a dummy _mi_report_crashed() in the present
  header file, and include it in Maria standalone programs.

  Some standalone Maria programs, but less numerous than above, use objects
  from ma_check.o like maria_repair(). This brings in linking dependencies of
  ma_check.o, especially _ma_killed_ptr() and
  _ma_check_print_info|warning|error() which are defined in ha_maria.o, which
  brings mysqld.o again. To avoid the problem, we define here other versions
  of those functions, for standalone programs. Those programs can use them by
  including this header file and defining MA_CHECK_STANDALONE to 1 (this
  additional step is to allow programs to include only _mi_report_crashed(),
  when they don't need ma_check.o objects).
*/

struct st_myisam_info;
typedef struct st_myisam_info MI_INFO;
void _mi_report_crashed(MI_INFO *file __attribute__((unused)),
                        const char *message __attribute__((unused)),
                        const char *sfile __attribute__((unused)),
                        uint sline __attribute__((unused)))
{
}


#if defined(MA_CHECK_STANDALONE) && (MA_CHECK_STANDALONE == 1)

/*
  Check if check/repair operation was killed by a signal
*/

static int not_killed= 0;

volatile int *_ma_killed_ptr(HA_CHECK *param __attribute__((unused)))
{
  return &not_killed;			/* always NULL */
}

	/* print warnings and errors */
	/* VARARGS */

void _ma_check_print_info(HA_CHECK *param __attribute__((unused)),
			 const char *fmt,...)
{
  va_list args;
  DBUG_ENTER("_ma_check_print_info");
  DBUG_PRINT("enter", ("format: %s", fmt));

  va_start(args,fmt);
  (void)(vfprintf(stdout, fmt, args));
  (void)(fputc('\n',stdout));
  va_end(args);
  DBUG_VOID_RETURN;
}

/* VARARGS */

void _ma_check_print_warning(HA_CHECK *param, const char *fmt,...)
{
  va_list args;
  DBUG_ENTER("_ma_check_print_warning");
  DBUG_PRINT("enter", ("format: %s", fmt));

  fflush(stdout);
  if (!param->warning_printed && !param->error_printed)
  {
    if (param->testflag & T_SILENT)
      fprintf(stderr,"%s: MARIA file %s\n",my_progname_short,
	      param->isam_file_name);
    param->out_flag|= O_DATA_LOST;
  }
  param->warning_printed=1;
  va_start(args,fmt);
  fprintf(stderr,"%s: warning: ",my_progname_short);
  (void)(vfprintf(stderr, fmt, args));
  (void)(fputc('\n',stderr));
  fflush(stderr);
  va_end(args);
  DBUG_VOID_RETURN;
}

/* VARARGS */

void _ma_check_print_error(HA_CHECK *param, const char *fmt,...)
{
  va_list args;
  DBUG_ENTER("_ma_check_print_error");
  DBUG_PRINT("enter", ("format: %s", fmt));

  fflush(stdout);
  if (!param->warning_printed && !param->error_printed)
  {
    if (param->testflag & T_SILENT)
      fprintf(stderr,"%s: MARIA file %s\n",my_progname_short,param->isam_file_name);
    param->out_flag|= O_DATA_LOST;
  }
  param->error_printed|=1;
  va_start(args,fmt);
  fprintf(stderr,"%s: error: ",my_progname_short);
  (void)vfprintf(stderr, fmt, args);
  (void)fputc('\n',stderr);
  fflush(stderr);
  va_end(args);
  DBUG_VOID_RETURN;
}

#endif
