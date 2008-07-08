/* Copyright (C) 2004-2007 MySQL AB

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
*/

/**
 * @file
 *
 * @brief Contains the snapshot backup algorithm driver.
 *
 * This file contains the snapshot backup algorithm (also called a "driver"
 * in the online backup terminology. The snapshot backup algorithm may be
 * used in place of an engine-specific driver if one does not exist or if
 * chosen by the user.
 *
 * The snapshot backup algorithm is a non-blocking algorithm that enables a
 * consistent read of the tables given at the start of the backup/restore 
 * process. This is accomplished by using a consistent snapshot transaction
 * and table locks. Once all of the data is backed up or restored, the locks 
 * are removed. The snapshot backup is a row-level backup and therefore does 
 * not backup the indexes or any of the engine-specific files.
 *
 * The classes in this file use the namespace "snapshot_backup" to distinguish
 * these classes from other backup drivers. The backup functionality is
 * contained in the backup class shown below. Similarly, the restore
 * functionality is contained in the restore class below.
 *
 * The format of the backup is the same as the default backup driver.
 * Please see <code> be_default.cc </code> for a complete description.
 */

#include "../mysql_priv.h"
#include "backup_engine.h"
#include "be_snapshot.h"
#include "backup_aux.h"

namespace snapshot_backup {

using backup::byte;
using backup::result_t;
using backup::version_t;
using backup::Table_list;
using backup::Table_ref;
using backup::Buffer;
using namespace backup;

/**
  Cleanup backup

  This method provides a means to stop a current backup by allowing
  the driver to shutdown gracefully. The method call ends the current
  transaction and closes the tables.
*/
result_t Backup::cleanup()
{
  DBUG_ENTER("Default_backup::cleanup()");
  DBUG_PRINT("backup",("Snapshot driver - stop backup"));
  if (m_cleanup)
  {
    m_cleanup= FALSE;
    locking_thd->lock_state= LOCK_DONE; // set lock done so destructor won't wait
    if (m_trans_start)
    {
      ha_autocommit_or_rollback(locking_thd->m_thd, 0);
      end_active_trans(locking_thd->m_thd);
      m_trans_start= FALSE;
    }
    if (tables_open)
    {
      if (hdl)
        default_backup::Backup::end_tbl_read();
      close_thread_tables(locking_thd->m_thd);
      tables_open= FALSE;
    }
  }
  DBUG_RETURN(OK);
}

/**
  Lock the tables

  This method creates the consistent read transaction and acquires the read
  lock.
*/
result_t Backup::lock()
{
  DBUG_ENTER("Snapshot_backup::lock()");
  /*
    We must fool the locking code to think this is a select because
    any other command type places the engine in a non-consistent read
    state. 
  */
  locking_thd->m_thd->lex->sql_command= SQLCOM_SELECT; 
  locking_thd->m_thd->lex->start_transaction_opt|=
    MYSQL_START_TRANS_OPT_WITH_CONS_SNAPSHOT;
  int res= begin_trans(locking_thd->m_thd);
  if (res)
    DBUG_RETURN(ERROR);
  m_trans_start= TRUE;
  locking_thd->lock_state= LOCK_ACQUIRED;
  DBUG_ASSERT(locking_thd->m_thd == current_thd);
  DEBUG_SYNC(locking_thd->m_thd, "after_backup_cs_locked");
  DBUG_RETURN(OK);
}

result_t Backup::get_data(Buffer &buf)
{
  result_t res;

  if (!tables_open && (locking_thd->lock_state == LOCK_ACQUIRED))
  {
    // The lex needs to be cleaned up between consecutive calls to 
    // open_and_lock_tables. Otherwise, open_and_lock_tables will try to open
    // previously opened views and crash.
    locking_thd->m_thd->lex->cleanup_after_one_table_open();
    open_and_lock_tables(locking_thd->m_thd, locking_thd->tables_in_backup);
    tables_open= TRUE;
  }
  if (locking_thd->lock_state == LOCK_ACQUIRED)
  {
    DBUG_ASSERT(locking_thd->m_thd == current_thd);
    DEBUG_SYNC(locking_thd->m_thd, "when_backup_cs_reading");
  }

  res= default_backup::Backup::get_data(buf);

  /*
    If this is the last table to be read, close the transaction
    and unlock the tables. This is indicated by the lock state
    being set to LOCK_SIGNAL from parent::get_data(). This is set
    after the last table is finished reading.
  */
  if ((locking_thd->lock_state == LOCK_SIGNAL) || m_cancel)
    cleanup();
  return(res);
}

} /* snapshot_backup namespace */


