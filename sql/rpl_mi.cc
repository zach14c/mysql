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

#include "rpl_mi.h"

#ifdef HAVE_REPLICATION

Master_info::Master_info()
   :Rpl_info("I/O"),
   ssl(0), ssl_verify_server_cert(0),
   port(MYSQL_PORT), connect_retry(DEFAULT_CONNECT_RETRY), 
   heartbeat_period(0), received_heartbeats(0),
   master_id(0)
{
  host[0] = 0; user[0] = 0; password[0] = 0;
  ssl_ca[0]= 0; ssl_capath[0]= 0; ssl_cert[0]= 0;
  ssl_cipher[0]= 0; ssl_key[0]= 0;
  my_init_dynamic_array(&ignore_server_ids, sizeof(::server_id), 16, 16);
}

Master_info::~Master_info()
{
  delete_dynamic(&ignore_server_ids);
}

/**
   A comparison function to be supplied as argument to @c sort_dynamic()
   and @c bsearch()

   @return -1 if first argument is less, 0 if it equal to, 1 if it is greater
   than the second
*/
int server_id_cmp(ulong *id1, ulong *id2)
{
  return *id1 < *id2? -1 : (*id1 > *id2? 1 : 0);
}


/**
   Reports if the s_id server has been configured to ignore events 
   it generates with

      CHANGE MASTER IGNORE_SERVER_IDS= ( list of server ids )

   Method is called from the io thread event receiver filtering.

   @param      s_id    the master server identifier

   @retval   TRUE    if s_id is in the list of ignored master  servers,
   @retval   FALSE   otherwise.
 */
bool Master_info::shall_ignore_server_id(ulong s_id)
{
  if (likely(ignore_server_ids.elements == 1))
    return (* (ulong*) dynamic_array_ptr(&ignore_server_ids, 0)) == s_id;
  else      
    return bsearch((const ulong *) &s_id,
                   ignore_server_ids.buffer,
                   ignore_server_ids.elements, sizeof(ulong),
                   (int (*) (const void*, const void*)) server_id_cmp)
      != NULL;
}

void init_master_log_pos(Master_info* mi)
{
  DBUG_ENTER("init_master_log_pos");

  mi->master_log_name[0] = 0;
  mi->master_log_pos = BIN_LOG_HEADER_SIZE;             // skip magic number
  /* 
    always request heartbeat unless master_heartbeat_period is set
    explicitly zero.  Here is the default value for heartbeat period
    if CHANGE MASTER did not specify it.  (no data loss in conversion
    as hb period has a max)
  */
  mi->heartbeat_period= (float) min(SLAVE_MAX_HEARTBEAT_PERIOD,
                                    (slave_net_timeout/2.0));
  DBUG_ASSERT(mi->heartbeat_period > (float) 0.001
              || mi->heartbeat_period == 0);
  DBUG_VOID_RETURN;
}

int Master_info::init_info(bool abort_if_no_info)
{
  int error= 0;

  DBUG_ENTER("Master_info::init_info");

  if (inited)
    DBUG_RETURN(error);

  if (!(abort_if_no_info && do_check()) &&
      !(error= do_init_info()))
    inited= 1;

  DBUG_RETURN (error);
}

void Master_info::inject_relay_log_info(Relay_log_info* info)
{
  rli= info;
}

#endif /* HAVE_REPLICATION */
