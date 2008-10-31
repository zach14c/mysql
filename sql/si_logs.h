#ifndef SI_LOGS_H_
#define SI_LOGS_H_

/**
   @file

   This file defines the API for the following object services:
     - table|file|both logging services for backup
*/

/**
  List of operations for backup history log.
*/
enum enum_backup_operation
{
  OP_BACKUP = 1,
  OP_RESTORE,
  OP_SHOW,
  OP_OTHER
};

/**
  List of states for backup logs.
*/
enum enum_backup_state
{
  BUP_UNKNOWN = 0,
  BUP_COMPLETE,
  BUP_STARTING,
  BUP_VALIDITY_POINT,
  BUP_RUNNING,
  BUP_ERRORS,
  BUP_CANCEL
};

/**
  Structure for holding backup history data.

  This structure is used to collect the information needed to write a
  single row of information to the backup_history log.
*/
struct st_backup_history
{
  ulonglong backup_id;             ///< the id for this row in the log
  int process_id;                  ///< the process id of the backup/restore
  enum_backup_state state;         ///< current state of the operation
  enum_backup_operation operation; ///< the type of operation (backup, restore)
  int error_num;                   ///< error number
  char *user_comment;              ///< user comment from command
  char *backup_file;               ///< the backup image file
  char backup_file_path[FN_REFLEN]; ///< the backup image path
  char *command;                   ///< the command used
  int binlog_pos;                  ///< position in the binary log
  char *binlog_file;               ///< the name of the binary log file
  int num_objects;                 ///< number of objects in backup
  longlong size;                   ///< total size of the backup image file
  time_t start;                    ///< start time of operation
  time_t stop;                     ///< stop time of operation
  time_t vp_time;                  ///< point in time validation was assured
  String driver_name;              ///< list of backup engines used
};


/**
  Class Backup_log defines the basic set of operations for server logs.

  This class is used to write information to the backup_history and 
  backup_progress logs. While the log output control is determined by the 
  server, this class is used as an interface to allow the backup to write 
  messages to the logs without regard to how they are stored or how the 
  logging mechanisms of the server behave.

  To use this class, one must instantiate the class. When instantiated, the
  constructor gets the next backup_id from the server and initializes the 
  history data stored in m_op_hist.

  Use the set methods below to store the information for the logs. When the
  caller is ready to write information to the logs, call the write_*() 
  method for the appropriate log.

  The write_history() method is used to write the history data to the 
  backup_history log. It should be called at the end of the backup or 
  restore operation.

  The write_progress() method is used to write miscellaneous messages to
  the backup_progress log. It may be called at any time.

  The state() method is designed to change the state of the operation and
  to write a message to the backup_progress log.

  @todo Add method to set the user comment from the command-line.
*/
class Backup_log
{
public:
  Backup_log(THD *thd, 
             enum_backup_operation type, 
             const char *query); 

  /* 
    Write the backup history data to the backup_history log. 
  */
  bool write_history();

  /* 
    Write a message to the backup_progress log.
  */
  bool write_progress(const char *object,
                      time_t start,
                      time_t stop,
                      longlong size,
                      longlong progress,
                      int error_num,
                      const char *notes);
  /*
    Check the backup logs (as tables).
  */
  bool check_logs();

  /*
    The following get/set methods populate the history data for
    the backup_history log.
  */
  ulonglong get_backup_id() { return m_op_hist.backup_id; }
  void state(enum_backup_state);
  void error_num(int code) { m_op_hist.error_num= code; }
  void binlog_pos(unsigned long int pos) { m_op_hist.binlog_pos= pos; }
  void binlog_file(char *file);
  void num_objects(int num) { m_op_hist.num_objects= num; }
  void size(longlong s) { m_op_hist.size= s; }
  void start(time_t when);
  void stop(time_t when);
  void vp_time(time_t when, bool report);
  void add_driver(const char* driver);
  void backup_file(const char *full_path);

private:
  st_backup_history m_op_hist;  ///< history log information
  THD *m_thd;                   ///< current thread

  /*
    Helper method to provide string constants for states.
  */
  const char *get_state_string(enum_backup_state state);
};

/**
  Report start of an operation.

  This method saves the start time in the history data.
  
  @param[IN]  when  The start time.

  @note If the time is 0|NULL, nothing is saved in the history data.
*/
inline
void Backup_log::start(time_t when)
{
  if (when)
    m_op_hist.start= when;
}

/**
  Report stop of an operation.

  This method saves the stop time in the history data.
  
  @param[IN]  when  The stop time.

  @note If the time is 0|NULL, nothing is saved in the history data.
*/
inline
void Backup_log::stop(time_t when)
{
  if (when)
    m_op_hist.stop= when;
}

/** 
  Report binlog position at validity point.

  This method saves the binlog file name in the history data.

  @param[IN] file Binlog file name.

  @note If the file name is 0|NULL, nothing is saved in the history data.
*/
inline
void Backup_log::binlog_file(char *file)
{
  if (strlen(file) > 0)
    m_op_hist.binlog_file= file;
}

#endif // SI_LOGS_H_
