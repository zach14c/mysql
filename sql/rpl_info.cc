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

#include <my_global.h>
#include "mysql_priv.h"
#include "rpl_info.h"

#ifdef HAVE_REPLICATION

Rpl_info::Rpl_info(const char* type)
    :Slave_reporting_capability(type),
    info_thd(0), inited(0), abort_slave(0),
    slave_running(0), slave_run_id(0)
{
  /*
    We have to use MYF_NO_DEADLOCK_DETECTION because mysqld doesn't
    lock run_lock and data_lock consistently.
    Should be fixed as this can easily lead to deadlocks
  */
  my_pthread_mutex_init(&run_lock, MY_MUTEX_INIT_FAST, 
                        "Master_info::run_lock", MYF_NO_DEADLOCK_DETECTION);
  my_pthread_mutex_init(&data_lock, MY_MUTEX_INIT_FAST,
                        "Master_info::data_lock", MYF_NO_DEADLOCK_DETECTION);
  pthread_cond_init(&data_cond, NULL);
  pthread_cond_init(&start_cond, NULL);
  pthread_cond_init(&stop_cond, NULL);

#ifdef SAFE_MUTEX
  /* Define mutex order for locks to find wrong lock usage */
  pthread_mutex_lock(&data_lock);
  pthread_mutex_lock(&run_lock);
  pthread_mutex_unlock(&run_lock);
  pthread_mutex_unlock(&data_lock);
#endif
}

Rpl_info::~Rpl_info()
{
  DBUG_ENTER("Rpl_info::~Rpl_info");

  pthread_mutex_destroy(&run_lock);
  pthread_mutex_destroy(&data_lock);
  pthread_cond_destroy(&data_cond);
  pthread_cond_destroy(&start_cond);
  pthread_cond_destroy(&stop_cond);

  DBUG_VOID_RETURN;
}
#endif /* HAVE_REPLICATION */
