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

#ifndef RPL_INFO_H
#define RPL_INFO_H

#include "rpl_reporting.h"

class Rpl_info : public Slave_reporting_capability
{
public:
  /*
    standard lock acquisition order to avoid deadlocks:
    run_lock, data_lock, relay_log.LOCK_log, relay_log.LOCK_index
  */
  pthread_mutex_t data_lock,run_lock;
  /*
    start_cond is broadcast when SQL thread is started
    stop_cond - when stopped
    data_cond - when data protected by data_lock changes
  */
  pthread_cond_t data_cond,start_cond,stop_cond;

  THD *info_thd; 

  /*
    inited changes its value within LOCK_active_mi-guarded critical
    sections  at times of start_slave_threads() (0->1) and end_slave() (1->0).
    Readers may not acquire the mutex while they realize potential concurrency
    issue.
  */
  volatile bool inited;
  volatile bool abort_slave;
  volatile uint slave_running;
  volatile ulong slave_run_id;

#ifndef DBUG_OFF
  int events_until_exit;
#endif

  inline int check()
  {
    return do_check();
  }
  
  inline int flush_info()
  {
    return do_flush_info();
  }
  
  inline int reset_info()
  {
    return do_reset_info();
  }
  
  inline void end_info()
  {
    do_end_info();
  }

  Rpl_info(const char* type);
  virtual ~Rpl_info();

private:
  virtual int do_check()= 0;
  virtual int do_init_info()= 0;
  virtual int do_flush_info()= 0;
  virtual void do_end_info()= 0;
  virtual int do_reset_info()= 0;

  Rpl_info& operator=(const Rpl_info& info);
  Rpl_info(const Rpl_info& info);
};

#endif /* RPL_INFO_H */
