#include "../mysql_priv.h"

#include "logger.h"
#include "catalog.h"

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

  @returns 0 on success.
 */
int Logger::write_message(log_level::value level, int error_code,
                          const char *msg)
{
   char buf[ERRMSGSIZE + 30];
   const char *out= msg;

   if (m_state == READY || m_state == RUNNING)
   {
     my_snprintf(buf,sizeof(buf),"%s: %s", 
                 m_type == BACKUP ? "Backup" : "Restore" , msg);
     out= buf;
   }
   
   switch (level) {
   case log_level::ERROR:
     if (m_save_errors)
       errors.push_front(new MYSQL_ERROR(::current_thd, error_code,
                                         MYSQL_ERROR::WARN_LEVEL_ERROR,msg));
     sql_print_error(out);
     DBUG_PRINT("backup_log",("[ERROR] %s",out));
     
     if (m_state == READY || m_state == RUNNING)
       report_ob_error(m_op_id, error_code);
     
     return 0;

   case log_level::WARNING:
     sql_print_warning(out);
     DBUG_PRINT("backup_log",("[Warning] %s",out));
     return 0;

   case log_level::INFO:
     sql_print_information(out);
     DBUG_PRINT("backup_log",("[Info] %s",out));
     return 0;

   default: return ERROR;
   }
}

/**
  Output message registered in errmsg.txt database.

  @param level       level of the message (INFO,WARNING,ERROR)
  @param error_code  code assigned to the message in errmsg.txt

  If the message contains placeholders, additional arguments provide
  values to be put there.

  @returns 0 on success.
 */
int Logger::v_report_error(log_level::value level, int error_code, va_list args)
{
  return v_write_message(level,error_code,ER_SAFE(error_code),args);
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

  my_vsnprintf(buf,sizeof(buf),format,args);
  return write_message(level,error_code,buf);
}

/**
  Report statistics from backup/restore catalogue before the main operation starts.
 */ 
void Logger::report_stats_pre(const Image_info &info)
{
  DBUG_ASSERT(m_state == RUNNING);
  
  report_ob_num_objects(m_op_id, info.table_count());
}

/**
  Report statistics from backup/restore catalogue after the operation is completed.
 */ 
void Logger::report_stats_post(const Image_info &info)
{
  DBUG_ASSERT(m_state == RUNNING);
  
  report_ob_size(m_op_id, info.data_size);
}

} // backup namespace
