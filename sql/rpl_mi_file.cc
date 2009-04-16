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

#include <my_global.h> // For HAVE_REPLICATION
#include "mysql_priv.h"
#include <my_dir.h>

#include <rpl_mi_file.h>

#ifdef HAVE_REPLICATION

// Defined in slave.cc
int init_intvar_from_file(int* var, IO_CACHE* f, int default_val);
int init_strvar_from_file(char *var, int max_size, IO_CACHE *f,
			  const char *default_val);
int init_floatvar_from_file(float* var, IO_CACHE* f, float default_val);
int init_dynarray_intvar_from_file(DYNAMIC_ARRAY* arr, IO_CACHE* f);

enum {
  LINES_IN_MASTER_INFO_WITH_SSL= 14,

  /* 5.1.16 added value of master_ssl_verify_server_cert */
  LINE_FOR_MASTER_SSL_VERIFY_SERVER_CERT= 15,

  /* 6.0 added value of master_heartbeat_period */
  LINE_FOR_MASTER_HEARTBEAT_PERIOD= 16,

  /* 6.0 added value of master_ignore_server_id */
  LINE_FOR_REPLICATE_IGNORE_SERVER_IDS= 17,

  /* Number of lines currently used when saving master info file */
  LINES_IN_MASTER_INFO= LINE_FOR_REPLICATE_IGNORE_SERVER_IDS
};

Master_info_file::Master_info_file(const char* param_info_fname)
  :info_fname(param_info_fname), info_fd(-1),
  sync_counter(0)
{
  DBUG_ENTER("Master_info_file::Master_info_file");

  bzero((char*) &info_file, sizeof(info_file));

  DBUG_VOID_RETURN;
}

int Master_info_file::do_check()
{
  char fname[FN_REFLEN+128];
  fn_format(fname, info_fname, mysql_data_home, "", 4+32);

  return (access(fname,F_OK));
}

int Master_info_file::do_init_info()
{
  int error;
  char fname[FN_REFLEN+128];
  DBUG_ENTER("Master_info_file::do_init_info");

  mysql= 0;
  file_id= 1;
  fn_format(fname, info_fname, mysql_data_home, "", 4+32);

  /* does master.info exist ? */
  if (access(fname,F_OK))
  {
    /*
      if someone removed the file from underneath our feet, just close
      the old descriptor and re-create the old file
    */
    if (info_fd >= 0)
      my_close(info_fd, MYF(MY_WME));
    if ((info_fd = my_open(fname, O_CREAT|O_RDWR|O_BINARY, MYF(MY_WME))) < 0 )
    {
      sql_print_error("Failed to create a new master info file (\
file '%s', errno %d)", fname, my_errno);
      goto err;
    }
    if (init_io_cache(&info_file, info_fd, IO_SIZE*2, READ_CACHE, 0L,0,
                      MYF(MY_WME)))
    {
      sql_print_error("Failed to create a cache on master info file (\
file '%s')", fname);
      goto err;
    }

    init_master_log_pos(this);
  }
  else // file exists
  {
    if (info_fd >= 0)
      reinit_io_cache(&info_file, READ_CACHE, 0L,0,0);
    else
    {
      if ((info_fd = my_open(fname, O_RDWR|O_BINARY, MYF(MY_WME))) < 0 )
      {
        sql_print_error("Failed to open the existing master info file (\
file '%s', errno %d)", fname, my_errno);
        goto err;
      }
      if (init_io_cache(&info_file, info_fd, IO_SIZE*2, READ_CACHE, 0L,
                        0, MYF(MY_WME)))
      {
        sql_print_error("Failed to create a cache on master info file (\
file '%s')", fname);
        goto err;
      }
    }

    int var_port, var_connect_retry, var_master_log_pos, lines;
    int var_ssl= 0, var_ssl_verify_server_cert= 0;
    float var_master_heartbeat_period= 0.0;
    char *first_non_digit;

    /*
       Starting from 4.1.x master.info has new format. Now its
       first line contains number of lines in file. By reading this
       number we will be always distinguish to which version our
       master.info corresponds to. We can't simply count lines in
       file since versions before 4.1.x could generate files with more
       lines than needed.
       If first line doesn't contain a number or contain number less than
       LINES_IN_MASTER_INFO_WITH_SSL then such file is treated like file
       from pre 4.1.1 version.
       There is no ambiguity when reading an old master.info, as before
       4.1.1, the first line contained the binlog's name, which is either
       empty or has an extension (contains a '.'), so can't be confused
       with an integer.

       So we're just reading first line and trying to figure which version
       is this.
    */

    /*
       The first row is temporarily stored in master_log_name,
       if it is line count and not binlog name (new format) it will be
       overwritten by the second row later.
    */
    if (init_strvar_from_file(master_log_name,
                              sizeof(master_log_name), &info_file,
                              ""))
      goto errwithmsg;

    lines= strtoul(master_log_name, &first_non_digit, 10);

    if (master_log_name[0]!='\0' &&
        *first_non_digit=='\0' && lines >= LINES_IN_MASTER_INFO_WITH_SSL)
    {
      /* Seems to be new format => read master log name from next line */
      if (init_strvar_from_file(master_log_name,
            sizeof(master_log_name), &info_file, ""))
        goto errwithmsg;
    }
    else
      lines= 7;

    if (init_intvar_from_file(&var_master_log_pos, &info_file, 4) ||
        init_strvar_from_file(host, sizeof(host), &info_file, 0) ||
        init_strvar_from_file(user, sizeof(user), &info_file, "test") ||
        init_strvar_from_file(password, SCRAMBLED_PASSWORD_CHAR_LENGTH+1,
                              &info_file, 0 ) ||
        init_intvar_from_file(&var_port, &info_file, MYSQL_PORT) ||
        init_intvar_from_file(&var_connect_retry, &info_file, DEFAULT_CONNECT_RETRY))
      goto errwithmsg;

    /*
      If file has ssl part use it even if we have server without
      SSL support. But these option will be ignored later when
      slave will try connect to master, so in this case warning
      is printed.
    */
    if (lines >= LINES_IN_MASTER_INFO_WITH_SSL)
    {
      if (init_intvar_from_file(&var_ssl, &info_file, 0) ||
          init_strvar_from_file(ssl_ca, sizeof(ssl_ca),
                                &info_file, 0) ||
          init_strvar_from_file(ssl_capath, sizeof(ssl_capath),
                                &info_file, 0) ||
          init_strvar_from_file(ssl_cert, sizeof(ssl_cert),
                                &info_file, 0) ||
          init_strvar_from_file(ssl_cipher, sizeof(ssl_cipher),
                                &info_file, 0) ||
          init_strvar_from_file(ssl_key, sizeof(ssl_key),
                               &info_file, 0))
      goto errwithmsg;

      /*
        Starting from 5.1.16 ssl_verify_server_cert might be
        in the file
      */
      if (lines >= LINE_FOR_MASTER_SSL_VERIFY_SERVER_CERT &&
          init_intvar_from_file(&var_ssl_verify_server_cert, &info_file, 0))
        goto errwithmsg;
      /*
        Starting from 6.0 master_heartbeat_period might be
        in the file
      */
      if (lines >= LINE_FOR_MASTER_HEARTBEAT_PERIOD &&
          init_floatvar_from_file(&var_master_heartbeat_period, &info_file, 0.0))
        goto errwithmsg;
      /*
        Starting from 6.0 list of server_id of ignorable servers might be
        in the file
      */
      if (lines >= LINE_FOR_REPLICATE_IGNORE_SERVER_IDS &&
          init_dynarray_intvar_from_file(&ignore_server_ids, &info_file))
      {
        sql_print_error("Failed to initialize master info ignore_server_ids");
        goto errwithmsg;
      }
    }

#ifndef HAVE_OPENSSL
    if (var_ssl)
      sql_print_warning("SSL information in the master info file "
                      "('%s') are ignored because this MySQL slave was "
                      "compiled without SSL support.", fname);
#endif /* HAVE_OPENSSL */

    /*
      This has to be handled here as init_intvar_from_file can't handle
      my_off_t types
    */
    master_log_pos= (my_off_t) var_master_log_pos;
    port= (uint) var_port;
    connect_retry= (uint) var_connect_retry;
    ssl= (my_bool) var_ssl;
    ssl_verify_server_cert= var_ssl_verify_server_cert;
    heartbeat_period= var_master_heartbeat_period;
  }
  DBUG_PRINT("master_info",("log_file_name: %s  position: %ld",
                            master_log_name,
                            (ulong) master_log_pos));

  // now change cache READ -> WRITE - must do this before flush_info
  reinit_io_cache(&info_file, WRITE_CACHE, 0L, 0, 1);
  if ((error=test(do_flush_info())))
    sql_print_error("Failed to flush master info file");
  DBUG_RETURN(error);

errwithmsg:
  sql_print_error("Error reading master configuration");

err:
  if (info_fd >= 0)
  {
    my_close(info_fd, MYF(0));
    end_io_cache(&info_file);
  }
  info_fd= -1;
  DBUG_RETURN(1);
}

/**
  Flushes the master info to a file.
  
  @retval 1 if it failed,
  @retval 0 otherwise.
*/
int Master_info_file::do_flush_info()
{
  IO_CACHE* file = &info_file;
  char lbuf[22];
  int err= 0;

  DBUG_ENTER("Master_info_file::do_flush_info");
  DBUG_PRINT("enter",("master_pos: %ld", (long) master_log_pos));

  /*
     In certain cases this code may create master.info files that seems
     corrupted, because of extra lines filled with garbage in the end
     file (this happens if new contents take less space than previous
     contents of file). But because of number of lines in the first line
     of file we don't care about this garbage.
  */
  char heartbeat_buf[sizeof(heartbeat_period) * 4]; // buffer to suffice always
  my_sprintf(heartbeat_buf, (heartbeat_buf, "%.3f", heartbeat_period));
  /*
    produce a line listing the total number and all the ignored server_id:s
  */
  char* ignore_server_ids_buf;
  {
    ignore_server_ids_buf=
      (char *) my_malloc((sizeof(::server_id) * 3 + 1) *
                         (1 + ignore_server_ids.elements), MYF(MY_WME));
    if (!ignore_server_ids_buf)
      DBUG_RETURN(1);
    for (ulong i= 0, cur_len= my_sprintf(ignore_server_ids_buf,
                                         (ignore_server_ids_buf, "%u",
                                          ignore_server_ids.elements));
         i < ignore_server_ids.elements; i++)
    {
      ulong s_id;
      get_dynamic(&ignore_server_ids, (uchar*) &s_id, i);
      cur_len +=my_sprintf(ignore_server_ids_buf + cur_len,
                           (ignore_server_ids_buf + cur_len,
                            " %lu", s_id));
    }
  }
  my_b_seek(file, 0L);
  my_b_printf(file,
              "%u\n%s\n%s\n%s\n%s\n%s\n%d\n%d\n%d\n%s\n%s\n%s\n%s\n%s\n%d\n%s\n%s\n",
              LINES_IN_MASTER_INFO,
              master_log_name, llstr(master_log_pos, lbuf),
              host, user,
              password, port, connect_retry,
              (int)(ssl), ssl_ca, ssl_capath, ssl_cert,
              ssl_cipher, ssl_key, ssl_verify_server_cert,
              heartbeat_buf,
              ignore_server_ids_buf);
  my_free(ignore_server_ids_buf, MYF(0));
  err= flush_io_cache(file);
  if (sync_masterinfo_period && !err && 
      ++(sync_counter) >= sync_masterinfo_period)
  {
    err= my_sync(info_fd, MYF(MY_WME));
    sync_counter= 0;
  }
  DBUG_RETURN(-err);
}

void Master_info_file::do_end_info()
{
  DBUG_ENTER("Master_info_file::do_end_info");

  if (!inited)
    DBUG_VOID_RETURN;

  if (info_fd >= 0)
  {
    end_io_cache(&info_file);
    (void)my_close(info_fd, MYF(MY_WME));
    info_fd = -1;
  }
  inited = 0;

  DBUG_VOID_RETURN;
}

int Master_info_file::do_reset_info() 
{
  MY_STAT stat_area;
  char fname[FN_REFLEN];
  int error= 0;

  fn_format(fname, info_fname, mysql_data_home, "", 4+32);
  if (my_stat(fname, &stat_area, MYF(0)) && my_delete(fname, MYF(MY_WME)))
    error= 1;

  return (error);
}
#endif /* HAVE_REPLICATION */
