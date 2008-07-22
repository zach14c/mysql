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
  @file 
 
  @brief Contains the nodata backup algorithm driver.
 
  This file contains the nodata backup algorithm (also called a "driver"
  in the online backup terminology. The nodata driver does not read or
  write to any files or tables. It is used to allow the backup and 
  restore of storage engines that do not store data. These include:

    DB_TYPE_MRG_MYISAM
    DB_TYPE_BLACKHOLE_DB
    DB_TYPE_FEDERATED_DB
    DB_TYPE_EXAMPLE_DB
*/
#include "../mysql_priv.h"
#include "backup_engine.h"
#include "be_nodata.h"
#include "backup_aux.h"

namespace nodata_backup {

using backup::byte;
using backup::result_t;
using backup::version_t;
using backup::Table_list;
using backup::Table_ref;
using backup::Buffer;

using namespace backup;

Engine::Engine(THD *t_thd)
{
  m_thd= t_thd;
}

/**
  Create a nodata backup backup driver.
  
  Creates a stubbed driver class for the backup kernel code. This
  allows the driver to be used in a backup while not reading data.
  
  @param[IN]  tables list of tables to be backed-up.
  @param[OUT] eng    pointer to backup driver instance.
  
  @retval  ERROR  if cannot create backup driver class.
  @retval  OK     on success.
*/
result_t Engine::get_backup(const uint32, const Table_list &tables,
                            Backup_driver* &drv)
{
  DBUG_ENTER("Engine::get_backup");
  Backup *ptr= new nodata_backup::Backup(tables);
  if (!ptr)
    DBUG_RETURN(ERROR);
  drv= ptr;
  DBUG_RETURN(OK);
}

/**
  @brief Get the data for a row in the table.

  This method is the main method used in the backup operation. It
  is stubbed and does not read any data.
*/
result_t Backup::get_data(Buffer &buf)
{
  DBUG_ENTER("Nodata_backup::get_data)");
  buf.table_num= 0;
  buf.size= 0;
  buf.last= TRUE;
  DBUG_RETURN(DONE);
}

/**
  Create a nodata backup restore driver.
  
  Creates a stubbed driver class for the backup kernel code. This
  allows the driver to be used in a restore while not writing data.
  
  @param[IN]  version  version of the backup image.
  @param[IN]  tables   list of tables to be restored.
  @param[OUT] eng      pointer to restore driver instance.
  
  @retval ERROR  if cannot create restore driver class.
  @retval OK     on success.
*/
result_t Engine::get_restore(version_t, const uint32, 
                             const Table_list &tables, Restore_driver* &drv)
{
  DBUG_ENTER("Engine::get_restore");
  Restore *ptr= new nodata_backup::Restore(tables, m_thd);
  if (!ptr)
    DBUG_RETURN(ERROR);
  drv= ptr;
  DBUG_RETURN(OK);
}

/**
   @brief Restore the data for a row in the table.
  
   This method is stubbed and does not write any data.
*/
result_t Restore::send_data(Buffer &buf)
{
  DBUG_ENTER("Nodata_backup::send_data)");
  buf.last= TRUE;
  buf.size= 0;
  buf.table_num= 0;
  DBUG_RETURN(DONE);
}

} /* nodata_backup namespace */


