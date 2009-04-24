/* Copyright (C) 2005 MySQL AB

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

#ifndef RPL_RLI_FILE_H
#define RPL_RLI_FILE_H

#include "rpl_rli.h"

class Relay_log_info_file : public Relay_log_info
{
  public:
  
  const char* info_fname;
  /*
    info_fd - file descriptor of the info file. set only during
    initialization or clean up - safe to read anytime
  */
  File info_fd;
  /* IO_CACHE of the info file - set only during init or end */
  IO_CACHE info_file;
  /*
   Keeps track of the number of transactions that commits
   before fsyncing. The option --sync-relay-log-info determines
   how many transactions should commit before fsyncing.
  */
  uint sync_counter;

  Relay_log_info_file(bool is_slave_recovery, const char* info_name);

private:
  int do_check();
  int do_init_info();
  int do_flush_info();
  void do_end_info();
  int do_reset_info();

  Relay_log_info_file& operator=(const Relay_log_info_file& info);
  Relay_log_info_file(const Relay_log_info_file& info);
};

#endif /* RPL_RLI_FILE_H */
