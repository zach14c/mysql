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

#ifndef RPL_MI_FILE_H
#define RPL_MI_FILE_H

#ifdef HAVE_REPLICATION

#include "rpl_mi.h"

class Master_info_file : public Master_info
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
   Keeps track of the number of events before fsyncing. The option 
   --sync-master-info determines how many events should be processed
   before fsyncing.
  */
  uint sync_counter;

  Master_info_file(const char* param_info_fname);

private:
  int do_check();
  int do_init_info();
  void do_end_info();
  int do_flush_info();
  int do_reset_info();

  Master_info_file& operator=(const Master_info_file& info);
  Master_info_file(const Master_info_file& info);
};

#endif /* HAVE_REPLICATION */
#endif /* RPL_MI_FILE_H */
