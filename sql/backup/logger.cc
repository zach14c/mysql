#include "../mysql_priv.h"

#include "logger.h"
#include "image_info.h"

/** @file
 
 @todo Log errors to progress tables
 */ 

namespace backup {

/**
  Output message on a given level.

  This is the low-level method used by all other methods to output messages.
  It's implementation determines how messages are delivered to the user.
  Currently they are appended to the server's error log.

  @param level       level of the message (INFO,WARNING,ERROR)
  @param error_code  optional code for message coming from errmsg.txt database -
                     for other messages set to 0
  @param msg         message text

  @note It should be possible to use this method (and other error reporting
  methods relying on it) right after creation of the Logger object instance.
  The message should be written to these destinations which are available at
  the moment. Destinations which are not ready/initialized yet should be 
  silently ignored.

  @returns Reported error code.
 */
int Logger::write_message(log_level::value level, int error_code,
                          const char *msg)
{
   char buf[ERRMSGSIZE + 30];
   /*
     When logging to server's error log, msg will be prefixed with
     "Backup:"/"Restore:" if the operation has been initialized (i.e., after
     Logger::init() call). For other destinations, msg is reported as it is.
     
     Pointer out points at output string for server's error log, which has the
     prefix added if needed.
    */ 
   const char *out= msg;

   /*
     Note: m_type is meaningful only after a call to init() i.e., 
     if m_state != CREATED.
   */ 
   if (m_state != CREATED)
   {
     my_snprintf(buf, sizeof(buf), "%s: %s", 
                 m_type == BACKUP ? "Backup" : "Restore" , msg);
     out= buf;
   }
   
   switch (level) {
   case log_level::ERROR:
   {
     // Report to server's error log

     sql_print_error("%s", out);

     // Report to the client

     bool saved_value= m_thd->no_warnings_for_error;
     m_thd->no_warnings_for_error= m_push_errors ? FALSE : TRUE;
     my_printf_error(error_code, msg, MYF(0));
     m_thd->no_warnings_for_error= saved_value;
 
     m_error_reported= TRUE;

     // Report to backup logs

     if (m_state == READY || m_state == RUNNING)
     {
       time_t ts = my_time(0);

       backup_log->error_num(error_code);
       backup_log->write_progress(0, ts, ts, 0, 0, error_code, msg);
     }
     
     // Report in the debug trace
     
     DBUG_PRINT("backup_log",("[ERROR] %s", out));
     
     return error_code;
   }

   case log_level::WARNING:
     // Report to server's error log
     sql_print_warning("%s", out);

     // Report to the client (push on warning stack)
     if (m_push_errors)
       push_warning_printf(current_thd, MYSQL_ERROR::WARN_LEVEL_WARN,
                           error_code, "%s", msg);

     // Report to the debug trace
     DBUG_PRINT("backup_log",("[Warning] %s", out));

     return error_code;

   case log_level::INFO:
     // Report to server's error log
     sql_print_information("%s", out);

     // Report to the debug trace
     DBUG_PRINT("backup_log",("[Info] %s", out));

     return error_code;

   default: DBUG_ASSERT(0); return ERROR;
   }
}

/**
  Output message registered in errmsg.txt database.

  @param level       level of the message (INFO,WARNING,ERROR)
  @param error_code  code assigned to the message in errmsg.txt

  If the message contains placeholders, additional arguments provide
  values to be put there.

  @returns Reported error code.
 */
int Logger::v_report_error(log_level::value level, int error_code, va_list args)
{
  return v_write_message(level, error_code, ER_SAFE(error_code), args);
}

/**
  Output unregistered message.

  Format string is given explicitly as an argument.

  Note: no localization support.
 */
int Logger::v_write_message(log_level::value level, int error_code,
                            const char *format, va_list args)
{
  char buf[ERRMSGSIZE + 20];

  my_vsnprintf(buf, sizeof(buf), format, args);
  return write_message(level, error_code, buf);
}

/**
  Report statistics from backup/restore catalogue before the main operation
  starts.
 */ 
void Logger::report_stats_pre(const Image_info &info)
{
  DBUG_ASSERT(m_state == RUNNING);
  backup_log->num_objects(info.table_count());
  // Compose list of databases.

  Image_info::Db_iterator *it= info.get_dbs();
  Image_info::Obj *db;
  Image_info::Obj::describe_buf name_buf;
  String dbs;

  while((db= (*it)++))
  {
    if (!dbs.is_empty())
      dbs.append(",");
    const size_t len= strnlen(db->describe(name_buf), sizeof(name_buf));
    /*
      If appending next database name would create too long string, append
      ellipsis instead and break the loop.
      
      The length limit 220-4 is computed as follows. The placeholder for 
      database list in ER_BACKUP_BACKUP/RESTORE_DBS has maximum width 220 from
      which we subtract 4 for ",...", which should fit if the loop iterates 
      once more.

      The width 220 for database list placeholder is chosen so that a complete 
      message fits into 256 characters.
    */
    if (dbs.length() + len > 220-4)
    {
      dbs.append("...");
      break;
    }
    dbs.append(name_buf);
  }

  delete it;

  // Log the databases.

  report_error(log_level::INFO, m_type == BACKUP ? ER_BACKUP_BACKUP_DBS
                                                 : ER_BACKUP_RESTORE_DBS, 
                                info.db_count(), dbs.c_ptr());
}

/**
  Report statistics from backup/restore catalogue after the operation is
  completed.
 */ 
void Logger::report_stats_post(const Image_info &info)
{
  DBUG_ASSERT(m_state == RUNNING);
  backup_log->size(info.data_size);
}

/*
 Indicate if reported errors should be pushed on the warning stack.

 If @c flag is TRUE, errors will be pushed on the warning stack, otherwise
 they will not.

 @returns Current setting.
*/
bool Logger::push_errors(bool flag)
{
  bool old= m_push_errors;
  m_push_errors= flag;
  return old;
} 

} // backup namespace
