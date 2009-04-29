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

#ifndef RPL_MI_H
#define RPL_MI_H

#ifdef HAVE_REPLICATION

#include "rpl_rli.h"
#include "my_sys.h"

#define DEFAULT_CONNECT_RETRY 60

/*****************************************************************************

  Replication IO Thread

  Master_info contains:
    - information about how to connect to a master
    - current master log name
    - current master log offset
    - misc control variables

  Master_info is initialized once from the master.info repository if such
  exists. Otherwise, data members corresponding to master.info fields
  are initialized with defaults specified by master-* options. The
  initialization is done through init_info() call.

  Logically, the format of master.info repository is presented as follows:

  log_name
  log_pos
  master_host
  master_user
  master_pass
  master_port
  master_connect_retry

  To write out the contents of master.info to disk a call to flush_info() 
  is required. Currently, it is needed every time we read and queue data 
  from the master.

  To clean up, call end_info()

*****************************************************************************/

class Master_info : public Rpl_info
{
 public:
  Master_info();
  virtual ~Master_info();
  bool shall_ignore_server_id(ulong s_id);

  /* the variables below are needed because we can change masters on the fly */
  char master_log_name[FN_REFLEN];
  char host[HOSTNAME_LENGTH+1];
  char user[USERNAME_LENGTH+1];
  char password[MAX_PASSWORD_LENGTH+1];
  my_bool ssl; // enables use of SSL connection if true
  char ssl_ca[FN_REFLEN], ssl_capath[FN_REFLEN], ssl_cert[FN_REFLEN];
  char ssl_cipher[FN_REFLEN], ssl_key[FN_REFLEN];
  my_bool ssl_verify_server_cert;

  my_off_t master_log_pos;

  MYSQL* mysql;
  uint32 file_id;				/* for 3.23 load data infile */
  Relay_log_info *rli;
  uint port;
  uint connect_retry;
  float heartbeat_period;         // interface with CHANGE MASTER or master.info
  ulonglong received_heartbeats;  // counter of received heartbeat events
  DYNAMIC_ARRAY ignore_server_ids;
  ulong master_id;
  /*
     The difference in seconds between the clock of the master and the clock of
     the slave (second - first). It must be signed as it may be <0 or >0.
     clock_diff_with_master is computed when the I/O thread starts; for this the
     I/O thread does a SELECT UNIX_TIMESTAMP() on the master.
     "how late the slave is compared to the master" is computed like this:
     clock_of_slave - last_timestamp_executed_by_SQL_thread - clock_diff_with_master

  */
  long clock_diff_with_master;

  int init_info(bool abort_if_no_info);
  void inject_relay_log_info(Relay_log_info *info);

private:
  virtual int do_check()= 0;
  virtual int do_init_info()= 0;
  virtual void do_end_info()= 0;
  virtual int do_flush_info()= 0;
  virtual int do_reset_info()= 0;

  Master_info& operator=(const Master_info& info);
  Master_info(const Master_info& info);
};

void init_master_log_pos(Master_info* mi);

int server_id_cmp(ulong *id1, ulong *id2);

#endif /* HAVE_REPLICATION */
#endif /* RPL_MI_H */
