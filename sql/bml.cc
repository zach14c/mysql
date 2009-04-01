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

   @brief Contains methods to implement the Backup Metadata Lock (BML) service.

   This file contains methods that allow SQL statements to register with the
   BML service. In case the lock is taken, these statements will be blocked.
   It also contains methods for taking and releasing the lock.

   The list of statements which should obey BML is as follows:

    DROP   DATABASE/TABLE/VIEW/FUNCTION/PROCEDURE/EVENT/TRIGGER/INDEX
    DROP   USER/TABLESPACE
    CREATE DATABASE/TABLE/VIEW/FUNCTION/PROCEDURE/EVENT/TRIGGER/INDEX
    ALTER  DATABASE/TABLE/VIEW/FUNCTION/PROCEDURE/EVENT/TABLESPACE
    RENAME TABLE/USER
    GRANT/REVOKE
    TRUNCATE/OPTIMIZE/REPAIR TABLE

   The parser (mysql_execute_command() in sql_parse.cc) arranges for calls to
   bml_enter() and bml_leave() for these statements.
*/

#include "bml.h"
#include "debug_sync.h"

BML_class *BML_class::m_instance= NULL;

BML_class *BML_class::get_BML_class_instance()
{
  if (m_instance == NULL)
    m_instance = new BML_class();
  return m_instance;
}

void BML_class::destroy_BML_class_instance()
{
  delete m_instance;
  m_instance= NULL;
}

BML_class::BML_class()
{
  pthread_mutex_init(&THR_LOCK_BML, MY_MUTEX_INIT_FAST);
  pthread_mutex_init(&THR_LOCK_BML_active, MY_MUTEX_INIT_FAST);
  pthread_mutex_init(&THR_LOCK_BML_get, MY_MUTEX_INIT_FAST);
  pthread_cond_init(&COND_BML, NULL);
  pthread_cond_init(&COND_BML_registered, NULL);
  pthread_cond_init(&COND_BML_release, NULL);
  BML_active= FALSE;
  BML_registered= 0;
}

BML_class::~BML_class()
{
  pthread_mutex_destroy(&THR_LOCK_BML);
  pthread_mutex_destroy(&THR_LOCK_BML_active);
  pthread_mutex_destroy(&THR_LOCK_BML_get);
  pthread_cond_destroy(&COND_BML);
  pthread_cond_destroy(&COND_BML_registered);
  pthread_cond_destroy(&COND_BML_release);
}

/**
   do_enter()

   Registers operation which obeys BML by increasing BML_registered counter.
*/
void BML_class::do_enter()
{
  DBUG_ENTER("BML_class::do_enter()");
  pthread_mutex_lock(&THR_LOCK_BML);
  BML_registered++;
  pthread_mutex_unlock(&THR_LOCK_BML);
  DBUG_VOID_RETURN;
}

/**
   bml_leave()

   Unregister operation which checked for BML with bml_enter(). 
   Decrements the BML_registered counter to indicate the operation 
   is done. Signals COND_BML_registered if counter == 0.
*/
void BML_class::bml_leave()
{
  DBUG_ENTER("BML_class::bml_leave()");
  pthread_mutex_lock(&THR_LOCK_BML);
  if (BML_registered > 0)
    BML_registered--;
  if (BML_registered == 0)
    pthread_cond_broadcast(&COND_BML_registered);
  pthread_mutex_unlock(&THR_LOCK_BML);
  DBUG_VOID_RETURN;
}

/**
   bml_enter

   Check to see if BML is active. If so, wait until it is deactivated.
   When BML is not active, register the operation with BML.

   If a timeout specified by backup_wait_timeout variable occurs, this
   method returns FALSE. The operation is not registered in that case.
    
   @param[in] thd        The THD object from the caller.

   @note: A successful call to bml_enter() must be matched by bml_leave().

   @returns TRUE if not blocked
   @returns FALSE if timeout occurs during the wait for BML release
*/
my_bool BML_class::bml_enter(THD *thd)
{
  int ret = 0;
  struct timespec ddl_timeout;
  DBUG_ENTER("BML_class::bml_enter()");

  set_timespec(ddl_timeout, thd->backup_wait_timeout);

  /*
    Check whether BML is active. If yes, wait for deactivation which is 
    signalled with COND_BML.
  */
  pthread_mutex_lock(&THR_LOCK_BML_active);
  thd->enter_cond(&COND_BML, &THR_LOCK_BML_active,
                  "BML: waiting until released");
  DEBUG_SYNC(thd, "bml_enter_check");
  while (BML_active && !thd->BML_exception && (ret == 0))
  {
    if (thd->backup_wait_timeout == 0)
      ret = -1;
    else
      ret= pthread_cond_timedwait(&COND_BML, &THR_LOCK_BML_active,
                                  &ddl_timeout);
  }
  thd->exit_cond("BML: entered");
  if (ret == 0)
    do_enter();
  else
    my_error(ER_DDL_TIMEOUT, MYF(0), thd->query);

  DBUG_RETURN(ret == 0);
}

/**
   bml_get

  This method is used to activate BML. It waits for any operations which
  registered with bml_enter() to unregister using bml_leave().
  The method also prevents any other thread from activating the lock until
  bml_release() is called.

  It checks the counter BML_registered and if > 0 it blocks the process until 
  all registerd operations are complete and the condition variable has been 
  signaled. The fact that BML is in force is indicated by setting the boolean 
  BML_active to TRUE.

   @params thd THD object.
   @returns TRUE
  */
my_bool BML_class::bml_get(THD *thd)
{
  DBUG_ENTER("BML_class::bml_get()");

  /*
    Only 1 thread can hold the BML. If BML_active is TRUE, wait for
    bml_release() which signals COND_BML_release condition.
  */
  pthread_mutex_lock(&THR_LOCK_BML_get);
  thd->enter_cond(&COND_BML_release, &THR_LOCK_BML_get,
                  "BML: witing for release before activating");
  DEBUG_SYNC(thd, "bml_get_check1");
  while (BML_active)
    pthread_cond_wait(&COND_BML_release,
                      &THR_LOCK_BML_get);
  BML_active= TRUE;
  thd->exit_cond("BML: activating");

  /*
    Wait for all registered statements to complete, i.e., until BML_registered
    is zero in which case COND_BML_registered is signalled.
  */
  pthread_mutex_lock(&THR_LOCK_BML);
  thd->enter_cond(&COND_BML_registered, &THR_LOCK_BML,
                  "BML: waiting for all statements to leave");
  DEBUG_SYNC(thd, "bml_get_check2");
  while (BML_registered != 0)
    pthread_cond_wait(&COND_BML_registered, &THR_LOCK_BML);
  thd->exit_cond("BML: activated");

  DEBUG_SYNC(thd, "after_bml_activated");
  DBUG_RETURN(TRUE);
}

/**
   bml_release

   This method is used to deactivate BML. All operations which are waiting
   in bml_enter() call (if any) will be allowed to continue.

   The BML_active flag is set to FALSE to indicate that BML is not active and
   conditions COND_BML and COND_BML_release are signalled.
*/
void BML_class::bml_release()
{
  pthread_mutex_lock(&THR_LOCK_BML);
  BML_active= FALSE;
  pthread_cond_broadcast(&COND_BML);
  pthread_cond_signal(&COND_BML_release);
  pthread_mutex_unlock(&THR_LOCK_BML);
}
