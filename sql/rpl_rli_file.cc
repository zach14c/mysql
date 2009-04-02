/* Copyright (C) 2000-2003 MySQL AB

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

#include "mysql_priv.h"

#include "rpl_mi.h"
#include "rpl_rli.h"
#include "rpl_rli_file.h"
#include <my_dir.h>    // For MY_STAT
#include "sql_repl.h"  // For check_binlog_magic
#include "rpl_utility.h"
#include "transaction.h"

// Defined in slave.cc
int init_intvar_from_file(int* var, IO_CACHE* f, int default_val);
int init_strvar_from_file(char *var, int max_size, IO_CACHE *f,
                          const char *default_val);

Relay_log_info_file::Relay_log_info_file(bool is_slave_recovery, const char* param_info_fname)
  :Relay_log_info(is_slave_recovery),
  info_fname(param_info_fname), info_fd(-1), sync_counter(0)
{
  DBUG_ENTER("Relay_log_info_file::Relay_log_info");

  bzero((char*) &info_file, sizeof(info_file));

  DBUG_VOID_RETURN;
}

int Relay_log_info_file::do_check()
{
  char fname[FN_REFLEN+128];
  fn_format(fname, info_fname, mysql_data_home, "", 4+32);

  return (access(fname,F_OK));
}

int Relay_log_info_file::do_init_info()
{
  char fname[FN_REFLEN+128];
  const char* msg = 0;
  int error = 0;
  DBUG_ENTER("Relay_log_info_file::do_init_info");
  DBUG_ASSERT(!no_storage);         // Don't init if there is no storage

  fn_format(fname, info_fname, mysql_data_home, "", 4+32);

  /* if file does not exist */
  if (access(fname,F_OK))
  {
    /*
      If someone removed the file from underneath our feet, just close
      the old descriptor and re-create the old file
    */
    if (info_fd >= 0)
      my_close(info_fd, MYF(MY_WME));
    if ((info_fd = my_open(fname, O_CREAT|O_RDWR|O_BINARY, MYF(MY_WME))) < 0)
    {
      sql_print_error("Failed to create a new relay log info file (\
file '%s', errno %d)", fname, my_errno);
      msg= current_thd->stmt_da->message();
      goto err;
    }
    if (init_io_cache(&info_file, info_fd, IO_SIZE*2, READ_CACHE, 0L,0,
                      MYF(MY_WME)))
    {
      sql_print_error("Failed to create a cache on relay log info file '%s'",
                      fname);
      msg= current_thd->stmt_da->message();
      goto err;
    }

    /* Init relay log with first entry in the relay index file */
    if (init_relay_log_pos(this,NullS,BIN_LOG_HEADER_SIZE,0 /* no data lock */,
                           &msg, 0))
    {
      sql_print_error("Failed to open the relay log 'FIRST' (relay_log_pos 4)");
      goto err;
    }
    group_master_log_name[0]= 0;
    group_master_log_pos= 0;
  }
  else // file exists
  {
    if (info_fd >= 0)
      reinit_io_cache(&info_file, READ_CACHE, 0L,0,0);
    else
    {
      int error= 0;
      if ((info_fd = my_open(fname, O_RDWR|O_BINARY, MYF(MY_WME))) < 0)
      {
        sql_print_error("\
Failed to open the existing relay log info file '%s' (errno %d)",
                        fname, my_errno);
        error= 1;
      }
      else if (init_io_cache(&info_file, info_fd,
                             IO_SIZE*2, READ_CACHE, 0L, 0, MYF(MY_WME)))
      {
        sql_print_error("Failed to create a cache on relay log info file '%s'",
                        fname);
        error= 1;
      }
      if (error)
      {
        msg= "Error openning slave log configuration";
        goto err;
      }
    }

    int relay_log_pos, master_log_pos;
    if (init_strvar_from_file(group_relay_log_name,
                              sizeof(group_relay_log_name),
                              &info_file, "") ||
       init_intvar_from_file(&relay_log_pos,
                             &info_file, BIN_LOG_HEADER_SIZE) ||
       init_strvar_from_file(group_master_log_name,
                             sizeof(group_master_log_name),
                             &info_file, "") ||
       init_intvar_from_file(&master_log_pos, &info_file, 0))
    {
      msg="Error reading slave log configuration";
      goto err;
    }
    strmake(event_relay_log_name,group_relay_log_name,
            sizeof(event_relay_log_name)-1);
    group_relay_log_pos= event_relay_log_pos= relay_log_pos;
    group_master_log_pos= master_log_pos;

    if (is_relay_log_recovery && init_recovery(mi, &msg))
      goto err;

    if (init_relay_log_pos(this,
                           group_relay_log_name,
                           group_relay_log_pos,
                           0 /* no data lock*/,
                           &msg, 0))
    {
      char llbuf[22];
      sql_print_error("Failed to open the relay log '%s' (relay_log_pos %s)",
                      group_relay_log_name,
                      llstr(group_relay_log_pos, llbuf));
      goto err;
    }
  }

#ifndef DBUG_OFF
  {
    char llbuf1[22], llbuf2[22];
    DBUG_PRINT("info", ("my_b_tell(cur_log)=%s event_relay_log_pos=%s",
                        llstr(my_b_tell(cur_log),llbuf1),
                        llstr(event_relay_log_pos,llbuf2)));
    DBUG_ASSERT(event_relay_log_pos >= BIN_LOG_HEADER_SIZE);
    DBUG_ASSERT((my_b_tell(cur_log) == event_relay_log_pos));
  }
#endif

  /*
    Now change the cache from READ to WRITE - must do this
    before flush_info
  */
  reinit_io_cache(&info_file, WRITE_CACHE,0L,0,1);
  if ((error= do_flush_info()))
  {
    msg= "Failed to flush relay log info file";
    goto err;
  }

  DBUG_RETURN(error);

err:
  sql_print_error(msg);
  end_io_cache(&info_file);
  if (info_fd >= 0)
    my_close(info_fd, MYF(0));
  info_fd= -1;
  DBUG_RETURN(1);
}


void Relay_log_info_file::do_end_info()
{
  DBUG_ENTER("Relay_log_info_file::do_end_info");

  if (info_fd >= 0)
  {
    end_io_cache(&info_file);
    (void) my_close(info_fd, MYF(MY_WME));
    info_fd = -1;
  }

  DBUG_VOID_RETURN;
}

/**
  Stores the file and position where the execute-slave thread are in the
  relay log:

    - As this is only called by the slave thread, we don't need to
      have a lock on this.
    - If there is an active transaction, then we don't update the position
      in the relay log.  This is to ensure that we re-execute statements
      if we die in the middle of an transaction that was rolled back.
    - As a transaction never spans binary logs, we don't have to handle the
      case where we do a relay-log-rotation in the middle of the transaction.
      If this would not be the case, we would have to ensure that we
      don't delete the relay log file where the transaction started when
      we switch to a new relay log file.

  @todo Change the log file information to a binary format to avoid calling
  longlong2str.

  @retval  0   ok,
  @retval  1   write error, otherwise.
*/

int Relay_log_info_file::do_flush_info()
{
  bool error=0;
  DBUG_ENTER("Relay_log_info_file::do_flush_info");

  if (unlikely(no_storage))
    DBUG_RETURN(0);

  IO_CACHE *file = &info_file;
  char buff[FN_REFLEN*2+22*2+4], *pos;

  my_b_seek(file, 0L);
  pos=strmov(buff, group_relay_log_name);
  *pos++='\n';
  pos=longlong2str(group_relay_log_pos, pos, 10);
  *pos++='\n';
  pos=strmov(pos, group_master_log_name);
  *pos++='\n';
  pos=longlong2str(group_master_log_pos, pos, 10);
  *pos='\n';
  if (my_b_write(file, (uchar*) buff, (size_t) (pos-buff)+1))
    error=1;
  if (flush_io_cache(file))
    error=1;
  if (sync_relayloginfo_period &&
      !error &&
      ++(sync_counter) >= sync_relayloginfo_period)
  {
    if (my_sync(info_fd, MYF(MY_WME)))
      error=0;
    sync_counter= 0;
  }
  /* Flushing the relay log is done by the slave I/O thread */
  DBUG_RETURN(error);
}

int Relay_log_info_file::do_reset_info()
{
  MY_STAT stat_area;
  char fname[FN_REFLEN];
  int error= 0;

  DBUG_ENTER("Relay_log_info_file::do_reset_info");

  fn_format(fname, info_fname, mysql_data_home, "", 4+32);
  if (my_stat(fname, &stat_area, MYF(0)) && my_delete(fname, MYF(MY_WME)))
    error= 1;

  DBUG_RETURN (error);
}
