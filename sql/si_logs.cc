/**
   @file

   This file defines the API for the following object services:
     - table|file|both logging services for backup

  The methods defined below are used to provide server functionality to
  and permitting an isolation layer for the client (caller).
*/

#include "mysql_priv.h"
#include "si_logs.h"
#include "log.h"

/**
  Initialize the class for logging backup or restore operation.
  
  This constructor initializes the m_op_hist structure with the
  information passed during instantiation.

  @param[in]  THD   current thread
  @param[in]  type  type of operation (backup or restore)
  @param[in]  path  location of the backup image
  @param[in]  query backup or restore query starting the operation
    
  @todo Add code to get the user comment from command.

*/ 
Backup_log::Backup_log(THD *thd, 
                       enum_backup_operation type, 
                       const LEX_STRING path, 
                       const char *query)
{
  ulonglong backup_id= 0;
  m_thd= thd;

  bzero(&m_op_hist, sizeof(st_backup_history));
  m_op_hist.process_id= m_thd->id;
  m_op_hist.state= BUP_STARTING;
  m_op_hist.operation= type;

  if (path.length > 0)
     m_op_hist.backup_file= path.str;

  if (strlen(query) > 0)
    m_op_hist.command= (char *)query;

  MYSQL_BACKUP_LOG *backup_log= logger.get_backup_history_log_file_handler();
  m_op_hist.backup_id= backup_log->get_next_backup_id();
}

/**
  Report name of a driver used in backup/restore operation.

  This method updates the drivers information in the history data. This method 
  appends to the those drivers listed in the history data.

  @param[IN] char *            driver_name  The name of the engine to add.
*/
void Backup_log::add_driver(const char *driver_name)
{
  String str;    // drivers string

  str.length(0);
  if (m_op_hist.driver_name.length())
    str.append(m_op_hist.driver_name);
  if ((str.length() > 0) && (strlen(driver_name) > 0))
    str.append(", ");
  if (strlen(driver_name) > 0)
    str.append(driver_name);
  m_op_hist.driver_name.copy(str);
}

/**
  Write history data.

  This method calls the server's logger to write the backup_history log
  information.

  @returns results of logging function (i.e., TRUE if error)

  @note This method should be called after all data has been set for the
  history data.
*/
bool Backup_log::write_history() 
{ 
  return logger.backup_history_log_write(m_thd, &m_op_hist); 
}

/**
  Write the backup log entry for the backup progress log.

  This method is a pass-through to allow calling of the logging 
  functions for the backup history log.

  @param[IN]   object      The name of the object processed
  @param[IN]   start       Start datetime
  @param[IN]   stop        Stop datetime
  @param[IN]   size        Size value
  @param[IN]   progress    Progress (percent)
  @param[IN]   error_num   Error number (should be 0 is success)
  @param[IN]   notes       Misc data from backup kernel

  @returns results of logging function (i.e., TRUE if error)
*/
bool Backup_log::write_progress(const char *object,
                                time_t start,
                                time_t stop,
                                longlong size,
                                longlong progress,
                                int error_num,
                                const char *notes)
{
  /* Write the message to the backup progress log */
  return logger.backup_progress_log_write(m_thd, m_op_hist.backup_id, object, 
                                          start, stop, size, progress, 
                                          error_num, notes);
}

/** 
  Report change of the state of operation
 
  For possible states see definition of @c enum_backup_state 

  @todo Consider reporting state changes in the server error log (as info
  entries).
 */
void Backup_log::state(enum_backup_state state)
{
  m_op_hist.state= state;
  logger.backup_progress_log_write(m_thd, m_op_hist.backup_id, "backup kernel", 0, 
                            0, 0, 0, 0, get_state_string(state));
}

/**
  Report validity point creation time.

  This method saves the validation point time in the history data and writes
  a message to the progress log.

  @param[IN]  when  Time of validity point.

  @note If the time is 0|NULL, nothing is saved in the history data.
*/
void Backup_log::vp_time(time_t when)
{
  if (when)
  {
    m_op_hist.vp_time= when; 
    logger.backup_progress_log_write(m_thd, m_op_hist.backup_id, "backup kernel", 
                                     when, 0, 0, 0, 0, "vp time");
  }
}

/**
  Get text string for state.

  @param[IN]  state  The current state of the operation

  @returns char * a text string for state.
*/
inline
const char *Backup_log::get_state_string(enum_backup_state state)
{
  switch (state) {
  case BUP_COMPLETE: return("complete");
  case BUP_STARTING: return("starting");
  case BUP_VALIDITY_POINT: return("validity point");
  case BUP_RUNNING: return("running");
  case BUP_ERRORS: return("error");
  case BUP_CANCEL: return("cancel");
  default: return("unknown");
  }
}
