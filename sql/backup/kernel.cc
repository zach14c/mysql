/**
  @file

  @brief Implementation of the backup kernel API.

  @section s1 How to use backup kernel API to perform backup and restore operations
  
  To perform backup or restore operation an appropriate context must be created.
  This involves creating required resources and correctly setting up the server.
  When operation is completed or interrupted, the context must be destroyed and
  all preparations reversed.
  
  All this is accomplished by creating an instance of Backup_create_ctx class and
  then using its methods to perform the operation. When the instance is 
  destroyed, the required clean-up is performed.
  
  This is how backup is performed using the context object:
  @code
  {
  
   Backup_restore_ctx context(thd); // create context instance
   Backup_info *info= context.prepare_for_backup(location, 
                                                 orig_loc); // prepare for backup
  
   // select objects to backup
   info->add_all_dbs();
   or
   info->add_dbs(<list of db names>);
  
   info->close(); // indicate that selection is done
  
   context.do_backup(); // perform backup
   
   context.close(); // explicit clean-up
  
  } // if code jumps here, context destructor will do the clean-up automatically
  @endcode
  
  Similar code will be used for restore (bit simpler as we don't support 
  selective restores yet):
  @code
  {
  
   Backup_restore_ctx context(thd); // create context instance
   Restore_info *info= context.prepare_for_restore(location,
                                                   orig_loc); // prepare for restore
  
   context.do_restore(); // perform restore
   
   context.close(); // explicit clean-up
  
  } // if code jumps here, context destructor will do the clean-up automatically
  @endcode

  @todo Use internal table name representation when passing tables to
        backup/restore drivers.
  @todo Handle other types of meta-data in Backup_info methods.
  @todo Handle item dependencies when adding new items.
  @todo Handle other kinds of backup locations (far future).
*/

#include "../mysql_priv.h"
#include "../si_objects.h"

#include "backup_kernel.h"
#include "backup_info.h"
#include "restore_info.h"
#include "logger.h"
#include "stream.h"
#include "be_native.h"
#include "be_default.h"
#include "be_snapshot.h"
#include "be_nodata.h"
#include "ddl_blocker.h"
#include "transaction.h"


/** 
  Global Initialization for online backup system.
 
  @note This function is called in the server initialization sequence, just
  after it loads all its plugins.
 */
int backup_init()
{
  pthread_mutex_init(&Backup_restore_ctx::run_lock, MY_MUTEX_INIT_FAST);
  Backup_restore_ctx::run_lock_initialized= TRUE;
  return 0;
}

/**
  Global clean-up for online backup system.
  
  @note This function is called in the server shut-down sequences, just before
  it shuts-down all its plugins.

  @note Due to way in which server's code is organized this function might be
  called and should work normally even in situation when backup_init() was not
  called at all.
 */
void backup_shutdown()
{
  if (Backup_restore_ctx::run_lock_initialized)
  {
    pthread_mutex_destroy(&Backup_restore_ctx::run_lock);
    Backup_restore_ctx::run_lock_initialized= FALSE;
  }
}

/*
  Forward declarations of functions used for sending response from BACKUP/RESTORE
  statement.
 */ 
static int send_error(Backup_restore_ctx &context, int error_code, ...);
static int send_reply(Backup_restore_ctx &context);


/**
  Call backup kernel API to execute backup related SQL statement.

  @param[IN] thd        current thread object reference.
  @param[IN] lex        results of parsing the statement.
  @param[IN] backupdir  value of the backupdir variable from server.
  @param[IN] overwrite  whether or not restore should overwrite existing
                        DB with same name as in backup image

  @note This function sends response to the client (ok, result set or error).

  @note Both BACKUP and RESTORE should perform implicit commit at the beginning
  and at the end of execution. This is done by the parser after marking these
  commands with appropriate flags in @c sql_command_flags[] in sql_parse.cc.

  @returns 0 on success, error code otherwise.
 */

int
execute_backup_command(THD *thd, LEX *lex, String *backupdir, bool overwrite)
{
  int res= 0;
  
  DBUG_ENTER("execute_backup_command");
  DBUG_ASSERT(thd && lex);
  DEBUG_SYNC(thd, "before_backup_command");

    
  using namespace backup;

  Backup_restore_ctx context(thd); // reports errors
  
  if (!context.is_valid())
    DBUG_RETURN(send_error(context, ER_BACKUP_CONTEXT_CREATE));

  /*
    Check backupdir for validity. This is needed since we cannot trust
    that the path is still valid. Access could have changed or the
    folders in the path could have been moved, deleted, etc.
  */
  if (backupdir->length() && my_access(backupdir->c_ptr(), (F_OK|W_OK)))
  {
    context.fatal_error(ER_BACKUP_BACKUPDIR, backupdir->c_ptr());
    DBUG_RETURN(send_error(context, ER_BACKUP_BACKUPDIR, backupdir->c_ptr()));
  }

  switch (lex->sql_command) {

  case SQLCOM_BACKUP:
  {
    // prepare for backup operation
    
    Backup_info *info= context.prepare_for_backup(backupdir, lex->backup_dir, 
                                                  thd->query,
                                                  lex->backup_compression);
                                                              // reports errors

    if (!info || !info->is_valid())
      DBUG_RETURN(send_error(context, ER_BACKUP_BACKUP_PREPARE));

    DEBUG_SYNC(thd, "after_backup_start_backup");

    // select objects to backup

    if (lex->db_list.is_empty())
    {
      context.write_message(log_level::INFO, "Backing up all databases");
      res= info->add_all_dbs(); // backup all databases
    }
    else
    {
      context.write_message(log_level::INFO, "Backing up selected databases");
      /* Backup databases specified by user. */
      res= info->add_dbs(thd, lex->db_list);
    }

    info->close(); // close catalogue after filling it with objects to backup

    if (res || !info->is_valid())
      DBUG_RETURN(send_error(context, ER_BACKUP_BACKUP_PREPARE));

    if (info->db_count() == 0)
    {
      context.fatal_error(ER_BACKUP_NOTHING_TO_BACKUP);
      DBUG_RETURN(send_error(context, ER_BACKUP_NOTHING_TO_BACKUP));
    }

    // perform backup

    res= context.do_backup();
 
    if (res)
      DBUG_RETURN(send_error(context, ER_BACKUP_BACKUP));

    break;
  }

  case SQLCOM_RESTORE:
  {

    /*
      Restore cannot be run on a slave while connected to a master.
    */
    if (obs::is_slave())
      DBUG_RETURN(send_error(context, ER_RESTORE_ON_SLAVE));

    Restore_info *info= context.prepare_for_restore(backupdir, lex->backup_dir, 
                                                    thd->query);
    
    if (!info || !info->is_valid())
      DBUG_RETURN(send_error(context, ER_BACKUP_RESTORE_PREPARE));
    
    DEBUG_SYNC(thd, "after_backup_start_restore");

    res= context.do_restore(overwrite);      

    DEBUG_SYNC(thd, "restore_before_end");

    if (res)
      DBUG_RETURN(send_error(context, ER_BACKUP_RESTORE));
    
    break;
  }

  default:
     /*
       execute_backup_command() should be called with correct command id
       from the parser. If not, we fail on this assertion.
      */
     DBUG_ASSERT(FALSE);

  } // switch(lex->sql_command)

  if (context.close())
    DBUG_RETURN(send_error(context, ER_BACKUP_CONTEXT_REMOVE));

  // All seems OK - send positive reply to client

  DBUG_RETURN(send_reply(context));
}

/**
  Report errors.

  Current implementation reports the last error saved in the logger if it exist.
  Otherwise it reports error given by @c error_code.

  @returns 0 on success, error code otherwise.
 */
int send_error(Backup_restore_ctx &log, int error_code, ...)
{
  util::SAVED_MYSQL_ERROR *error= log.last_saved_error();

  if (error && !util::report_mysql_error(log.thd(), error, error_code))
  {
    if (error->code)
      error_code= error->code;
  }
  else // there are no error information in the logger - report error_code
  {
    char buf[ERRMSGSIZE + 20];
    va_list args;
    va_start(args, error_code);

    my_vsnprintf(buf, sizeof(buf), ER_SAFE(error_code), args);
    my_printf_error(error_code, buf, MYF(0));

    va_end(args);
  }

  if (log.backup::Logger::m_state == backup::Logger::RUNNING)
    log.report_stop(my_time(0), FALSE); // FASLE = no success
  return error_code;
}


/**
  Send positive reply after a backup/restore operation.

  Currently the id of the operation is returned to the client. It can
  be used to select correct entries from the backup progress tables.

  @returns 0 on success, error code otherwise.
*/
int send_reply(Backup_restore_ctx &context)
{
  Protocol *protocol= context.thd()->protocol;    // client comms
  List<Item> field_list;                // list of fields to send
  char buf[255];                        // buffer for llstr

  DBUG_ENTER("send_reply");

  /*
    Send field list.
  */
  if (field_list.push_back(new Item_empty_string(STRING_WITH_LEN("backup_id"))))
  {
    goto err;
  }
  if (protocol->send_result_set_metadata(&field_list,
                            Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF))
  {
    goto err;
  }

  /*
    Send field data.
  */
  protocol->prepare_for_resend();               // Never errors
  llstr(context.op_id(), buf);                  // Never errors
  if (protocol->store(buf, system_charset_info))
  {
    goto err;
  }
  if (protocol->write())
  {
    goto err;
  }
  my_eof(context.thd());                        // Never errors
  DBUG_RETURN(0);

 err:
  DBUG_RETURN(context.fatal_error(ER_BACKUP_SEND_REPLY,
                                  context.m_type == backup::Logger::BACKUP
                                  ? "BACKUP" : "RESTORE"));
}


namespace backup {

/**
  This class provides memory allocation services for backup stream library.

  An instance of this class is created during preparations for backup/restore
  operation. When it is deleted, all allocated memory is freed.
*/
class Mem_allocator
{
 public:

  Mem_allocator();
  ~Mem_allocator();

  void* alloc(size_t);
  void  free(void*);

 private:

  struct node;
  node *first;  ///< Pointer to the first segment in the list.
};


} // backup namespace


/*************************************************

   Implementation of Backup_restore_ctx class

 *************************************************/

// static members

Backup_restore_ctx *Backup_restore_ctx::current_op= NULL;
bool Backup_restore_ctx::run_lock_initialized= FALSE;
pthread_mutex_t Backup_restore_ctx::run_lock;


Backup_restore_ctx::Backup_restore_ctx(THD *thd)
 :Logger(thd), m_state(CREATED), m_thd_options(thd->options),
  m_error(0), m_remove_loc(FALSE), m_stream(NULL),
  m_catalog(NULL), mem_alloc(NULL), m_tables_locked(FALSE),
  m_engage_binlog(FALSE)
{
  /*
    Check for progress tables.
  */
  MYSQL_BACKUP_LOG *backup_log= logger.get_backup_history_log_file_handler();
  if (backup_log->check_backup_logs(thd))
    m_error= ER_BACKUP_PROGRESS_TABLES;
}

Backup_restore_ctx::~Backup_restore_ctx()
{
  close();

  delete mem_alloc;
  delete m_catalog;  
  delete m_stream;
}

/**
  Prepare path for access.

  This method takes the backupdir and the file name specified on the backup
  command (orig_loc) and forms a combined path + file name as follows:

    1. If orig_loc has a relative path, make it relative to backupdir
    2. If orig_loc has a hard path, use it.
    3. If orig_loc has no path, append to backupdir

  @param[IN]  backupdir  The backupdir system variable value.
  @param[IN]  orig_loc   The path + file name specified in the backup command.

  @returns 0
*/
int Backup_restore_ctx::prepare_path(::String *backupdir, 
                                     LEX_STRING orig_loc)
{
  char fix_path[FN_REFLEN]; 
  char full_path[FN_REFLEN]; 

  /*
    Prepare the path using the backupdir iff no relative path
    or no hard path included.

    Relative paths are formed from the backupdir system variable.

    Case 1: Backup image file name has relative path. 
            Make relative to backupdir.

    Example BACKUP DATATBASE ... TO '../monthly/dec.bak'
            If backupdir = '/dev/daily' then the
            calculated path becomes
            '/dev/monthly/dec.bak'

    Case 2: Backup image file name has no path or has a subpath. 

    Example BACKUP DATABASE ... TO 'week2.bak'
            If backupdir = '/dev/weekly/' then the
            calculated path becomes
            '/dev/weekly/week2.bak'
    Example BACKUP DATABASE ... TO 'jan/day1.bak'
            If backupdir = '/dev/monthly/' then the
            calculated path becomes
            '/dev/monthly/jan/day1.bak'

    Case 3: Backup image file name has hard path. 

    Example BACKUP DATATBASE ... TO '/dev/dec.bak'
            If backupdir = '/dev/daily/backup' then the
            calculated path becomes
            '/dev/dec.bak'
  */

  /*
    First, we construct the complete path from backupdir.
  */
  fn_format(fix_path, backupdir->ptr(), mysql_real_data_home, "", 
            MY_UNPACK_FILENAME | MY_RELATIVE_PATH);

  /*
    Next, we contruct the full path to the backup file.
  */
  fn_format(full_path, orig_loc.str, fix_path, "", 
            MY_UNPACK_FILENAME | MY_RELATIVE_PATH);

  /*
    Copy result to member variable for Stream class.
  */
  m_path.copy(full_path, strlen(full_path), system_charset_info);
  report_backup_file(m_path.c_ptr());
 
  return 0;
}

/**
  Do preparations common to backup and restore operations.
  
  It is checked if another operation is in progress and if yes then
  error is reported. Otherwise the current operation is registered so that
  no other can be started. All preparations common to backup and restore 
  operations are done. In particular, all changes to meta data are blocked
  with DDL blocker.

  @returns 0 on success, error code otherwise.
 */ 
int Backup_restore_ctx::prepare(String *backupdir, LEX_STRING location)
{
  if (m_error)
    return m_error;
  
  // Prepare error reporting context.
  
  m_thd->warning_info->opt_clear_warning_info(m_thd->query_id); // Never errors
  m_thd->no_warnings_for_error= FALSE;

  save_errors();                                // Never errors


  /*
    Check access for SUPER rights. If user does not have SUPER, fail with error.
  */
  if (check_global_access(m_thd, SUPER_ACL))
  {
    fatal_error(ER_SPECIFIC_ACCESS_DENIED_ERROR, "SUPER");
    return m_error;
  }

  /*
    Check if another BACKUP/RESTORE is running and if not, register 
    this operation.
   */

  pthread_mutex_lock(&run_lock);

  if (!current_op)
    current_op= this;
  else
    fatal_error(ER_BACKUP_RUNNING);

  pthread_mutex_unlock(&run_lock);

  if (m_error)
    return m_error;

  // check if location is valid (we assume it is a file path)

  /*
    For this error to work correctly, we need to check original
    file specified by the user rather than the path formed
    using the backupdir.
  */
  bool bad_filename= (location.length == 0);
  
  /*
    On some systems certain file names are invalid. We use 
    check_if_legal_filename() function from mysys to detect this.
   */ 
#if defined(__WIN__) || defined(__EMX__)  

  bad_filename = bad_filename || check_if_legal_filename(location.str);

#endif

  if (bad_filename)
  {
    fatal_error(ER_BAD_PATH, location.str);
    return m_error;
  }

  /*
    Computer full path to backup file.
  */
  prepare_path(backupdir, location);

  // create new instance of memory allocator for backup stream library

  using namespace backup;

  delete mem_alloc;
  mem_alloc= new Mem_allocator();

  if (!mem_alloc)
  {
    fatal_error(ER_OUT_OF_RESOURCES);
    return m_error;
  }

  // Freeze all meta-data. 

  if (obs::ddl_blocker_enable(m_thd))
  {
    fatal_error(ER_DDL_BLOCK);
    return m_error;
  }

  return 0;
}

/**
  Prepare for backup operation.
  
  @param[in] backupdir  path to the file where backup image should be stored
  @param[in] orig_loc   path as specified on command line for backup image
  @param[in] query      BACKUP query starting the operation
  @param[in] with_compression  backup image compression switch
  
  @returns Pointer to a @c Backup_info instance which can be used for selecting
  which objects to backup. NULL if an error was detected.
  
  @note This function reports errors.

  @note It is important that changes of meta-data are blocked as part of the
  preparations. The set of server objects and their definitions should not
  change after the backup context has been prepared and before the actual backup
  is performed using @c do_backup() method.
 */ 
Backup_info* 
Backup_restore_ctx::prepare_for_backup(String *backupdir, 
                                       LEX_STRING orig_loc, 
                                       const char *query,
                                       bool with_compression)
{
  using namespace backup;
  
  if (m_error)
    return NULL;
  
  if (Logger::init(BACKUP, query))
  {
    fatal_error(ER_BACKUP_LOGGER_INIT);
    return NULL;
  }

  time_t when= my_time(0);
  report_start(when);
  
  /*
    Do preparations common to backup and restore operations. After call
    to prepare() all meta-data changes are blocked.
   */ 
  if (prepare(backupdir, orig_loc))
    return NULL;

  /*
    Open output stream.
   */
  Output_stream *s= new Output_stream(*this, 
                                      &m_path,
                                      with_compression);
  m_stream= s;
  
  if (!s)
  {
    fatal_error(ER_OUT_OF_RESOURCES);
    return NULL;
  }
  
  int my_open_status= s->open();
  if (my_open_status != 0)
  {
    report_stream_open_failure(my_open_status, &orig_loc);
    return NULL;
  }

  /*
    Create backup catalogue.
   */

  Backup_info *info= new Backup_info(*this);    // Logs errors

  if (!info)
  {
    fatal_error(ER_OUT_OF_RESOURCES);
    return NULL;
  }

  if (!info->is_valid())
    return NULL;    // Error has been logged by Backup_Info constructor

  /*
    If binlog is enabled, set BSTREAM_FLAG_BINLOG in the header to indicate
    that validity point's binlog position will be stored in the image 
    (in its summary section).
    
    This is not completely safe because theoretically even if now binlog is 
    active, it can be disabled before we reach the validity point and then we 
    will not store binlog position even though the flag is set. To fix this 
    problem the format of backup image must be changed (some flags must be 
    stored in the summary section which is written at the end of backup 
    operation).
  */
  if (mysql_bin_log.is_open())
    info->flags|= BSTREAM_FLAG_BINLOG; 

  info->save_start_time(when);
  m_catalog= info;
  m_state= PREPARED_FOR_BACKUP;
  
  return info;
}

/**
  Prepare for restore operation.
  
  @param[in] backupdir  path to the file where backup image is stored
  @param[in] orig_loc   path as specified on command line for backup image
  @param[in] query      RESTORE query starting the operation
  
  @returns Pointer to a @c Restore_info instance containing catalogue of the
  backup image (read from the image). NULL if errors were detected.
  
  @note This function reports errors.
 */ 
Restore_info* 
Backup_restore_ctx::prepare_for_restore(String *backupdir,
                                        LEX_STRING orig_loc, 
                                        const char *query)
{
  using namespace backup;  

  if (m_error)
    return NULL;
  
  if (Logger::init(RESTORE, query))
  {
    fatal_error(ER_BACKUP_LOGGER_INIT);
    return NULL;
  }

  time_t when= my_time(0);  
  report_start(when);

  /*
    Do preparations common to backup and restore operations. After this call
    changes of meta-data are blocked.
   */ 
  if (prepare(backupdir, orig_loc))
    return NULL;
  
  /*
    Open input stream.
   */

  Input_stream *s= new Input_stream(*this, &m_path);
  m_stream= s;
  
  if (!s)
  {
    fatal_error(ER_OUT_OF_RESOURCES);
    return NULL;
  }
  
  int my_open_status= s->open();
  if (my_open_status != 0)
  {
    report_stream_open_failure(my_open_status, &orig_loc);
    return NULL;
  }

  /*
    Create restore catalogue.
   */

  Restore_info *info= new Restore_info(*this);  // reports errors

  if (!info)
  {
    fatal_error(ER_OUT_OF_RESOURCES);
    return NULL;
  }

  if (!info->is_valid())
    return NULL;

  info->save_start_time(when);
  m_catalog= info;

  /*
    Read catalogue from the input stream.
   */

  if (read_header(*info, *s))
  {
    fatal_error(ER_BACKUP_READ_HEADER);
    return NULL;
  }

  if (s->next_chunk() != BSTREAM_OK)
  {
    fatal_error(ER_BACKUP_NEXT_CHUNK);
    return NULL;
  }

  if (read_catalog(*info, *s))
  {
    fatal_error(ER_BACKUP_READ_HEADER);
    return NULL;
  }

  if (s->next_chunk() != BSTREAM_OK)
  {
    fatal_error(ER_BACKUP_NEXT_CHUNK);
    return NULL;
  }

  m_state= PREPARED_FOR_RESTORE;

  /*
    Do not allow slaves to connect during a restore.

    If the binlog is turned on, write a RESTORE_EVENT as an
    incident report into the binary log.

    Turn off binlog during restore.
  */
  if (obs::is_binlog_engaged())
  {
    obs::disable_slave_connections(TRUE);

    DEBUG_SYNC(m_thd, "after_disable_slave_connections");

    obs::write_incident_event(m_thd, obs::RESTORE_EVENT);
    m_engage_binlog= TRUE;
    obs::engage_binlog(FALSE);
  }

  return info;
}

/*
  Lock tables being restored.

  Backup kernel ensures that all tables being restored are exclusively locked.

  We use open_and_lock_tables() for locking. This is a temporary solution until
  a better mechanism is devised - open_and_lock_tables() is not good if there
  are many tables to be processed.

  The built-in restore drivers need to open tables to write rows to them. Since
  we have opened tables here, we store pointers to opened TABLE_LIST structures
  in the restore catalogue so that the built-in drivers can access them later.

  @todo Replace open_and_lock_tables() by a lighter solution.
  @todo Hide table locking behind the server API.
*/ 
int Backup_restore_ctx::lock_tables_for_restore()
{
  TABLE_LIST *tables= NULL;

  /*
    Iterate over all tables in all snapshots and create a linked TABLE_LIST
    for call to open_and_lock_tables(). Store pointers to TABLE_LIST structures
    in the restore catalogue for later access to opened tables.
  */ 

  for (uint s= 0; s < m_catalog->snap_count(); ++s)
  {
    backup::Snapshot_info *snap= m_catalog->m_snap[s];

    for (ulong t=0; t < snap->table_count(); ++t)
    {
      backup::Image_info::Table *tbl= snap->get_table(t);
      DBUG_ASSERT(tbl); // All tables should be present in the catalogue.

      TABLE_LIST *ptr= backup::mk_table_list(*tbl, TL_WRITE, m_thd->mem_root);
      if (!ptr)
      {
        // Error has been reported, but not logged to backup logs
        return log_error(ER_OUT_OF_RESOURCES);
      }

      tables= backup::link_table_list(*ptr, tables); // Never errors
      tbl->m_table= ptr;
    }
  }

  /*
    Open and lock the tables.
    
    Note 1: It is important to not do derived tables processing here. Processing
    derived tables even leads to crashes as those reported in BUG#34758.
  
    Note 2: Skiping tmp tables is also important because otherwise a tmp table
    can occlude a regular table with the same name (BUG#33574).
  */ 
  if (open_and_lock_tables_derived(m_thd, tables,
                                   FALSE, /* do not process derived tables */
                                   MYSQL_OPEN_SKIP_TEMPORARY 
                                          /* do not open tmp tables */
                                  )
     )
  {
    fatal_error(ER_BACKUP_OPEN_TABLES,"RESTORE");
    return m_error;
  }

  m_tables_locked= TRUE;
  return 0;
}

/**
  Unlock tables which were locked by @c lock_tables_for_restore.
 */ 
void Backup_restore_ctx::unlock_tables()
{
  // Do nothing if tables are not locked.
  if (!m_tables_locked)
    return;

  DBUG_PRINT("restore",("unlocking tables"));

  close_thread_tables(m_thd);                   // Never errors
  m_tables_locked= FALSE;

  return;
}


/**
  Report error and move context object into error state without pushing the 
  error on the server's warning stack.  
  
  Similar to @c fatal_error, but does not push the error on the
  server's warning stack.  To be used when an error is reported from a
  server function that has already pushed the error on the warning stack.
  
  @return error code given as input or stored in the context object if
  a fatal error was reported before.
 */ 
inline
int Backup_restore_ctx::log_error(int error_code, ...)
{
  if (m_error)
    return m_error;

  bool saved = push_errors(FALSE);         // Do not use warning stack
  
  m_error= error_code;
  m_remove_loc= TRUE;

  va_list args;
  va_start(args,error_code);
  v_report_error(backup::log_level::ERROR, error_code, args);
  va_end(args);

  push_errors(saved);                      // Reset

  return error_code;
}


/**
  Destroy a backup/restore context.
  
  This should reverse all settings made when context was created and prepared.
  If it was requested, the backup/restore location is removed. Also, the backup
  stream memory allocator is shut down. Any other allocated resources are 
  deleted in the destructor. Changes to meta-data are unblocked.
  
  @returns 0 or error code if error was detected.
  
  @note This function reports errors.
 */ 
int Backup_restore_ctx::close()
{
  int error= 0;
  if (m_state == CLOSED)
    return 0;

  using namespace backup;

  /*
    Allow slaves connect after restore is complete.
  */
  obs::disable_slave_connections(FALSE);

  /*
    Turn binlog back on iff it was turned off earlier.
  */
  if (m_engage_binlog)
    obs::engage_binlog(TRUE);

  time_t when= my_time(0);

  // unlock tables if they are still locked
  unlock_tables();                              // Never errors

  // unfreeze meta-data
  obs::ddl_blocker_disable();                   // Never errors

  // restore thread options

  m_thd->options= m_thd_options;

  // close stream

  if (m_stream && !m_stream->close())
  {
    // Note error, but complete clean-up
    error= ER_BACKUP_CLOSE;
  }

  if (m_catalog)
    m_catalog->save_end_time(when); // Note: no errors.

  /* 
    Remove the location, if asked for.
    
    Important: This is done only for backup operation - RESTORE should never
    remove the specified backup image!
   */
  if (m_remove_loc && m_state == PREPARED_FOR_BACKUP)
  {
    int res= my_delete(m_path.c_ptr(), MYF(0));

    /*
      Ignore ENOENT error since it is ok if the file doesn't exist.
     */
    if (res && my_errno != ENOENT)
    {
      error= ER_CANT_DELETE_FILE;
    }
  }

  /* We report completion of the operation only if no errors were detected,
     and logger has been initialized.
  */
  if (!error)
  {
    if (backup::Logger::m_state == backup::Logger::RUNNING)
    {
      report_stop(when, TRUE);
    }
  }
  else
  {
    fatal_error(error);                         // Log error
  }

  /* 
    Destroy backup stream's memory allocator (this frees memory)
  
    Note that from now on data stored in this object might be corrupted. For 
    example the binlog file name is a string stored in memory allocated by
    the allocator which will be freed now.
  */
  
  delete mem_alloc;
  mem_alloc= NULL;
  
  // deregister this operation if it was running
  pthread_mutex_lock(&run_lock);
  if (current_op == this) {
    current_op= NULL;
  }
  pthread_mutex_unlock(&run_lock);

  m_state= CLOSED;
  return error;
}

/**
  Create backup archive.
  
  @pre @c prepare_for_backup() method was called.

  @returns 0 on success, error code otherwise.
*/
int Backup_restore_ctx::do_backup()
{
  DBUG_ENTER("do_backup");

  // This function should not be called when context is not valid
  DBUG_ASSERT(is_valid());
  DBUG_ASSERT(m_state == PREPARED_FOR_BACKUP);
  DBUG_ASSERT(m_thd);
  DBUG_ASSERT(m_stream);
  DBUG_ASSERT(m_catalog);
  
  using namespace backup;

  Output_stream &s= *static_cast<Output_stream*>(m_stream);
  Backup_info   &info= *static_cast<Backup_info*>(m_catalog);

  DEBUG_SYNC(m_thd, "before_backup_meta");

  report_stats_pre(info);                       // Never errors

  DBUG_PRINT("backup",("Writing preamble"));
  DEBUG_SYNC(m_thd, "backup_before_write_preamble");

  if (write_preamble(info, s))
  {
    fatal_error(ER_BACKUP_WRITE_HEADER);
    DBUG_RETURN(m_error);
  }

  DBUG_PRINT("backup",("Writing table data"));

  DEBUG_SYNC(m_thd, "before_backup_data");

  if (write_table_data(m_thd, info, s)) // logs errors
    DBUG_RETURN(send_error(*this, ER_BACKUP_BACKUP));

  DBUG_PRINT("backup",("Writing summary"));

  if (write_summary(info, s))
  {
    fatal_error(ER_BACKUP_WRITE_SUMMARY);
    DBUG_RETURN(m_error);
  }

  report_stats_post(info);                      // Never errors

  DBUG_PRINT("backup",("Backup done."));
  DEBUG_SYNC(m_thd, "before_backup_done");

  DBUG_RETURN(0);
}

/**
  Create all triggers and events from restore catalogue.

  This helper method iterates over all triggers and events stored in the 
  restore catalogue and creates them. When metadata section of the backup image 
  is read, trigger and event objects are materialized and stored in the 
  catalogue but they are not executed then (see @c bcat_create_item()). 
  This method can be used to re-create the corresponding server objects after 
  all other objects and table data have been restored.

  Note that we first restore all triggers and then the events.

  @returns 0 on success, error code otherwise.
*/ 
int Backup_restore_ctx::restore_triggers_and_events()
{
  using namespace backup;

  DBUG_ASSERT(m_catalog);

  DBUG_ENTER("restore_triggers_and_events");

  Image_info::Obj *obj;
  List<Image_info::Obj> events;
  Image_info::Obj::describe_buf buf;

  Image_info::Iterator *dbit= m_catalog->get_dbs();
  if (!dbit)
  {
    DBUG_RETURN(fatal_error(ER_OUT_OF_RESOURCES));
  }

  // create all trigers and collect events in the events list
  
  while ((obj= (*dbit)++)) 
  {
    Image_info::Iterator *it=
                    m_catalog->get_db_objects(*static_cast<Image_info::Db*>(obj));
    if (!it)
    {
      DBUG_RETURN(fatal_error(ER_OUT_OF_RESOURCES));
    }
    while ((obj= (*it)++))
      switch (obj->type()) {
      
      case BSTREAM_IT_EVENT:
        DBUG_ASSERT(obj->m_obj_ptr);
        if (events.push_back(obj))
        {
          // Error has been reported, but not logged to backup logs
          DBUG_RETURN(log_error(ER_OUT_OF_RESOURCES)); 
        }
        break;
      
      case BSTREAM_IT_TRIGGER:
        DBUG_ASSERT(obj->m_obj_ptr);
        if (obj->m_obj_ptr->create(m_thd))
        {
          delete it;
          delete dbit;
          fatal_error(ER_BACKUP_CANT_RESTORE_TRIGGER,obj->describe(buf));
          DBUG_RETURN(m_error);
        }
        break;

      default: break;      
      }

    delete it;
  }

  delete dbit;

  // now create all events

  List_iterator<Image_info::Obj> it(events);
  Image_info::Obj *ev;

  while ((ev= it++)) 
    if (ev->m_obj_ptr->create(m_thd))
    {
      fatal_error(ER_BACKUP_CANT_RESTORE_EVENT,ev->describe(buf));
      DBUG_RETURN(m_error);
    };

  DBUG_RETURN(0);
}

/**
  Restore objects saved in backup image.

  @pre @c prepare_for_restore() method was called.

  @param[IN] overwrite whether or not restore should overwrite existing
                       DB with same name as in backup image

  @returns 0 on success, error code otherwise.

  @todo Remove the @c reset_diagnostic_area() hack.
*/
int Backup_restore_ctx::do_restore(bool overwrite)
{
  DBUG_ENTER("do_restore");

  DBUG_ASSERT(is_valid());
  DBUG_ASSERT(m_state == PREPARED_FOR_RESTORE);
  DBUG_ASSERT(m_thd);
  DBUG_ASSERT(m_stream);
  DBUG_ASSERT(m_catalog);

  using namespace backup;

  int err;
  Input_stream &s= *static_cast<Input_stream*>(m_stream);
  Restore_info &info= *static_cast<Restore_info*>(m_catalog);

  report_stats_pre(info);                       // Never errors

  DBUG_PRINT("restore", ("Restoring meta-data"));

  // unless RESTORE... OVERWRITE: return error if database already exists
  if (!overwrite) {
    Image_info::Db_iterator *dbit= info.get_dbs();

    if (!dbit) {
      DBUG_RETURN(fatal_error(ER_OUT_OF_RESOURCES));
    }

    Image_info::Db *mydb;
    while ((mydb= static_cast<Image_info::Db*>((*dbit)++))) {
      if (!obs::check_db_existence(m_thd, &mydb->name())) {
        delete dbit;
        DBUG_RETURN(fatal_error(ER_RESTORE_DB_EXISTS, mydb->name().ptr()));
      }
    }
    delete dbit;
  }

  disable_fkey_constraints();                   // Never errors

  if (read_meta_data(info, s))
  {
    m_thd->stmt_da->reset_diagnostics_area();    // Never errors

    fatal_error(ER_BACKUP_READ_META);
    DBUG_RETURN(m_error);
  }

  if (s.next_chunk() == BSTREAM_ERROR)
  {
    DBUG_RETURN(fatal_error(ER_BACKUP_NEXT_CHUNK));
  }

  DBUG_PRINT("restore",("Restoring table data"));

  /* 
    FIXME: this call is here because object services doesn't clean the
    statement execution context properly, which leads to assertion failure.
    It should be fixed inside object services implementation and then the
    following line should be removed.
   */
  close_thread_tables(m_thd);                   // Never errors
  m_thd->stmt_da->reset_diagnostics_area();      // Never errors  

  if (lock_tables_for_restore())                // logs errors
    DBUG_RETURN(m_error);

  // Here restore drivers are created to restore table data
  err= restore_table_data(m_thd, info, s); // reports errors

  unlock_tables();                              // Never errors

  if (err)
    DBUG_RETURN(ER_BACKUP_RESTORE);

  /* 
   Re-create all triggers and events (it was not done in @c bcat_create_item()).

   Note: it is important to do that after tables are unlocked, otherwise 
   creation of these objects will fail.
  */

  if (restore_triggers_and_events())    // reports errors
     DBUG_RETURN(ER_BACKUP_RESTORE);

  DBUG_PRINT("restore",("Done."));

  if (read_summary(info, s))
  {
    fatal_error(ER_BACKUP_READ_SUMMARY);
    DBUG_RETURN(m_error);
  }

  /* 
    FIXME: this call is here because object services doesn't clean the
    statement execution context properly, which leads to assertion failure.
    It should be fixed inside object services implementation and then the
    following line should be removed.
   */
  close_thread_tables(m_thd);                   // Never errors
  m_thd->stmt_da->reset_diagnostics_area();      // Never errors

  /*
    Report validity point time and binlog position stored in the backup image
    (in the summary section).
   */ 

  report_vp_time(info.get_vp_time(), FALSE); // FALSE = do not write to progress log
  if (info.flags & BSTREAM_FLAG_BINLOG)
    report_binlog_pos(info.binlog_pos);

  report_stats_post(info);                      // Never errors

  DEBUG_SYNC(m_thd, "before_restore_done");

  DBUG_RETURN(0);
}

/**
  Report stream open error by calling fatal_error, effectively moving
  context object into error state.
  
  @return error code given as input or the one stored in the context
  object if a fatal error has already been reported.
 */ 
int Backup_restore_ctx::report_stream_open_failure(int my_open_status,
                                                   const LEX_STRING *location)
{
  int error= 0;
  switch (my_open_status) {
    case ER_OPTION_PREVENTS_STATEMENT:
      error= fatal_error(ER_OPTION_PREVENTS_STATEMENT, "--secure-file-priv");
      break;
    case ER_BACKUP_WRITE_LOC:
      /*
        For this error, use the actual value returned instead of the
        path complimented with backupdir.
      */
      error= fatal_error(ER_BACKUP_WRITE_LOC, location->str);
      break;
    case ER_BACKUP_READ_LOC:
      /*
        For this error, use the actual value returned instead of the
        path complimented with backupdir.
      */
      error= fatal_error(ER_BACKUP_READ_LOC, location->str);
      break;
    default:
      DBUG_ASSERT(FALSE);
  }
  return error;
}

namespace backup {

/*************************************************

    Implementation of Mem_allocator class.

 *************************************************/

/// All allocated memory segments are linked into a list using this structure.
struct Mem_allocator::node
{
  node *prev;
  node *next;
};

Mem_allocator::Mem_allocator() :first(NULL)
{}

/// Deletes all allocated segments which have not been freed explicitly.
Mem_allocator::~Mem_allocator()
{
  node *n= first;

  while (n)
  {
    first= n->next;
    my_free(n, MYF(0));
    n= first;
  }
}

/**
  Allocate memory segment of given size.

  Extra memory is allocated for @c node structure which holds pointers
  to previous and next segment in the segments list. This is used when
  deallocating allocated memory in the destructor.
*/
void* Mem_allocator::alloc(size_t howmuch)
{
  void *ptr= my_malloc(sizeof(node) + howmuch, MYF(0));

  if (!ptr)
    return NULL;

  node *n= (node*)ptr;
  ptr= n + 1;

  n->prev= NULL;
  n->next= first;
  if (first)
    first->prev= n;
  first= n;

  return ptr;
}

/**
  Explicit deallocation of previously allocated segment.

  The @c ptr should contain an address which was obtained from
  @c Mem_allocator::alloc().

  The deallocated fragment is removed from the allocated fragments list.
*/
void Mem_allocator::free(void *ptr)
{
  if (!ptr)
    return;

  node *n= ((node*)ptr) - 1;

  if (first == n)
    first= n->next;

  if (n->prev)
    n->prev->next= n->next;

  if (n->next)
    n->next->prev= n->prev;

  my_free(n, MYF(0));
}

} // backup namespace


/*************************************************

               CATALOGUE SERVICES

 *************************************************/

/**
  Memory allocator for backup stream library.

  @pre A backup/restore context has been created and prepared for the 
  operation (one of @c Backup_restore_ctx::prepare_for_backup() or 
  @c Backup_restore_ctx::prepare_for_restore() have been called).
 */
extern "C"
bstream_byte* bstream_alloc(unsigned long int size)
{
  using namespace backup;

  DBUG_ASSERT(Backup_restore_ctx::current_op 
              && Backup_restore_ctx::current_op->mem_alloc);

  return (bstream_byte*)Backup_restore_ctx::current_op->mem_alloc->alloc(size);
}

/**
  Memory deallocator for backup stream library.
*/
extern "C"
void bstream_free(bstream_byte *ptr)
{
  using namespace backup;
  if (Backup_restore_ctx::current_op 
      && Backup_restore_ctx::current_op->mem_alloc)
    Backup_restore_ctx::current_op->mem_alloc->free(ptr);
}

/**
  Prepare restore catalogue for populating it with items read from
  backup image.

  At this point we know the list of table data snapshots present in the image
  (it was read from image's header). Here we create @c Snapshot_info object
  for each of them.

  @rerturns 0 on success, error code otherwise.
*/
extern "C"
int bcat_reset(st_bstream_image_header *catalogue)
{
  using namespace backup;

  uint n;

  DBUG_ASSERT(catalogue);
  Restore_info *info= static_cast<Restore_info*>(catalogue);

  /*
    Iterate over the list of snapshots read from the backup image (and stored
    in snapshot[] array in the catalogue) and for each snapshot create a 
    corresponding Snapshot_info instance. A pointer to this instance is stored
    in m_snap[] array.
   */ 

  for (n=0; n < info->snap_count(); ++n)
  {
    st_bstream_snapshot_info *snap= &info->snapshot[n];

    DBUG_PRINT("restore",("Creating info for snapshot no. %d", n));

    switch (snap->type) {

    case BI_NATIVE:
    {
      backup::LEX_STRING name_lex(snap->engine.name.begin, snap->engine.name.end);
      storage_engine_ref se= get_se_by_name(name_lex);
      handlerton *hton= se_hton(se);

      if (!se || !hton)
      {
        info->m_ctx.fatal_error(ER_BACKUP_CANT_FIND_SE, name_lex.str);
        return BSTREAM_ERROR;
      }

      if (!hton->get_backup_engine)
      {
        info->m_ctx.fatal_error(ER_BACKUP_NO_NATIVE_BE, name_lex.str);
        return BSTREAM_ERROR;
      }

      info->m_snap[n]= new Native_snapshot(info->m_ctx, snap->version, se);
                                                              // reports errors
      break;
    }

    case BI_NODATA:
      info->m_snap[n]= new Nodata_snapshot(info->m_ctx, snap->version);
                                                              // reports errors
      break;

    case BI_CS:
      info->m_snap[n]= new CS_snapshot(info->m_ctx, snap->version);
                                                              // reports errors
      break;

    case BI_DEFAULT:
      info->m_snap[n]= new Default_snapshot(info->m_ctx, snap->version);
                                                              // reports errors
      break;

    default:
      // note: we use convention that snapshots are counted starting from 1.
      info->m_ctx.fatal_error(ER_BACKUP_UNKNOWN_BE, n + 1);
      return BSTREAM_ERROR;
    }

    if (!info->m_snap[n])
    {
      info->m_ctx.fatal_error(ER_OUT_OF_RESOURCES);
      return BSTREAM_ERROR;
    }

    info->m_snap[n]->m_num= n + 1;
    info->m_ctx.report_driver(info->m_snap[n]->name());
  }

  return BSTREAM_OK;
}

/**
  Called after reading backup image's catalogue and before processing
  metadata and table data.

  Nothing to do here.
*/
extern "C"
int bcat_close(st_bstream_image_header *catalogue)
{
  return BSTREAM_OK;
}

/**
  Add item to restore catalogue.

  @todo Report errors.
*/
extern "C"
int bcat_add_item(st_bstream_image_header *catalogue, 
                  struct st_bstream_item_info *item)
{
  using namespace backup;

  Restore_info *info= static_cast<Restore_info*>(catalogue);

  backup::String name_str(item->name.begin, item->name.end);

  DBUG_PRINT("restore",("Adding item %s of type %d (pos=%ld)",
                        item->name.begin,
                        item->type,
                        item->pos));

  switch (item->type) {

  case BSTREAM_IT_TABLESPACE:
  {
    Image_info::Ts *ts= info->add_ts(name_str, item->pos); // reports errors

    return ts ? BSTREAM_OK : BSTREAM_ERROR;
  }

  case BSTREAM_IT_DB:
  {
    Image_info::Db *db= info->add_db(name_str, item->pos); // reports errors

    return db ? BSTREAM_OK : BSTREAM_ERROR;
  }

  case BSTREAM_IT_TABLE:
  {
    st_bstream_table_info *it= (st_bstream_table_info*)item;

    DBUG_PRINT("restore",(" table's snapshot no. is %d", it->snap_num));

    Snapshot_info *snap= info->m_snap[it->snap_num];

    if (!snap)
    {
      /* 
        This can happen only if the snapshot number is too big - if we failed
        to create one of the snapshots listed in image's header we would stop
        with error earlier.
       */
      DBUG_ASSERT(it->snap_num >= info->snap_count());
      info->m_ctx.fatal_error(ER_BACKUP_WRONG_TABLE_BE, it->snap_num + 1);
      return BSTREAM_ERROR;
    }

    Image_info::Db *db= info->get_db(it->base.db->base.pos); // reports errors

    if (!db)
      return BSTREAM_ERROR;

    DBUG_PRINT("restore",(" table's database is %s", db->name().ptr()));

    Image_info::Table *tbl= info->add_table(*db, name_str, *snap, item->pos); 
                                                             // reports errors
    
    return tbl ? BSTREAM_OK : BSTREAM_ERROR;
  }

  case BSTREAM_IT_VIEW:
  case BSTREAM_IT_SPROC:
  case BSTREAM_IT_SFUNC:
  case BSTREAM_IT_EVENT:
  case BSTREAM_IT_TRIGGER:
  case BSTREAM_IT_PRIVILEGE:
  {
    st_bstream_dbitem_info *it= (st_bstream_dbitem_info*)item;
    
    DBUG_ASSERT(it->db);
    
    Image_info::Db *db= (Image_info::Db*) info->get_db(it->db->base.pos);
  
    DBUG_ASSERT(db);
    
    Image_info::Dbobj *it1= info->add_db_object(*db, item->type, name_str,
                                                item->pos);
    if (!it1)
      return BSTREAM_ERROR;
    
    return BSTREAM_OK;
  }   

  default:
    return BSTREAM_OK;

  } // switch (item->type)
}

/*****************************************************************

   Iterators

 *****************************************************************/

static uint cset_iter;  ///< Used to implement trivial charset iterator.
static uint null_iter;  ///< Used to implement trivial empty iterator.

/// Return pointer to an instance of iterator of a given type.
extern "C"
void* bcat_iterator_get(st_bstream_image_header *catalogue, unsigned int type)
{
  typedef backup::Image_info::Iterator Iterator; // to save some typing

  DBUG_ASSERT(catalogue);

  Backup_info *info= static_cast<Backup_info*>(catalogue);

  switch (type) {

  case BSTREAM_IT_PERTABLE: // per-table objects
    return &null_iter;

  case BSTREAM_IT_CHARSET:  // character sets
    cset_iter= 0;
    return &cset_iter;

  case BSTREAM_IT_USER:     // users
    return &null_iter;

  case BSTREAM_IT_TABLESPACE:     // table spaces
  {
    Iterator *it= info->get_tablespaces();
    if (!it) 
    {
      info->m_ctx.fatal_error(ER_OUT_OF_RESOURCES);
      return NULL;
    }
  
    return it;
  }

  case BSTREAM_IT_DB:       // all databases
  {
    Iterator *it= info->get_dbs();
    if (!it) 
    {
      info->m_ctx.fatal_error(ER_OUT_OF_RESOURCES);
      return NULL;
    }

    return it;  
  }
  
  case BSTREAM_IT_PERDB:    // per-db objects, except tables
  {
    Iterator *it= info->get_perdb();
  
    if (!it)
    {
      info->m_ctx.fatal_error(ER_BACKUP_CAT_ENUM);
      return NULL;
    }

    return it;
  }

  case BSTREAM_IT_GLOBAL:   // all global objects
  {
    Iterator *it= info->get_global();

    return it;      // if (!it), error has been logged in get_global()
  }

  default:
    return NULL;

  }
}

/// Return next item pointed by a given iterator and advance it to the next positon.
extern "C"
struct st_bstream_item_info*
bcat_iterator_next(st_bstream_image_header *catalogue, void *iter)
{
  using namespace backup;

  /* If this is the null iterator, return NULL immediately */
  if (iter == &null_iter)
    return NULL;

  static bstream_blob name= {NULL, NULL};

  /*
    If it is cset iterator then cset_iter variable contains iterator position.
    We return only 2 charsets: the utf8 charset used to encode all strings and
    the default server charset.
  */
  if (iter == &cset_iter)
  {
    switch (cset_iter) {
      case 0: name.begin= (backup::byte*)my_charset_utf8_bin.csname; break;
      case 1: name.begin= (backup::byte*)system_charset_info->csname; break;
      default: name.begin= NULL; break;
    }

    name.end= name.begin ? name.begin + strlen((char*)name.begin) : NULL;
    cset_iter++;

    return name.begin ? (st_bstream_item_info*)&name : NULL;
  }

  /*
    In all other cases assume that iter points at instance of
    @c Image_info::Iterator and use this instance to get next item.
   */
  const Image_info::Obj *ptr= (*(Image_info::Iterator*)iter)++;

  return ptr ? (st_bstream_item_info*)(ptr->info()) : NULL;
}

extern "C"
void  bcat_iterator_free(st_bstream_image_header *catalogue, void *iter)
{
  /*
    Do nothing for the null and cset iterators, but delete the
    @c Image_info::Iterator object otherwise.
  */
  if (iter == &null_iter)
    return;

  if (iter == &cset_iter)
    return;

  delete (backup::Image_info::Iterator*)iter;
}

/* db-items iterator */

/** 
  Return pointer to an iterator for iterating over objects inside a given 
  database.
 */
extern "C"
void* bcat_db_iterator_get(st_bstream_image_header *catalogue,
                           st_bstream_db_info *dbi)
{
  DBUG_ASSERT(catalogue);
  DBUG_ASSERT(dbi);
  
  Backup_info *info= static_cast<Backup_info*>(catalogue);
  Backup_info::Db *db = info->get_db(dbi->base.pos);

  if (!db)
  {
    info->m_ctx.fatal_error(ER_BACKUP_UNKNOWN_OBJECT);
    return NULL;
  }

  backup::Image_info::Iterator *it= info->get_db_objects(*db);
  if (!it)
  {
    info->m_ctx.fatal_error(ER_OUT_OF_RESOURCES);
    return NULL;
  }

  return it;
}

extern "C"
struct st_bstream_dbitem_info*
bcat_db_iterator_next(st_bstream_image_header *catalogue,
                      st_bstream_db_info *db,
                      void *iter)
{
  const backup::Image_info::Obj *ptr= (*(backup::Image_info::Iterator*)iter)++;

  return ptr ? (st_bstream_dbitem_info*)ptr->info() : NULL;
}

extern "C"
void  bcat_db_iterator_free(st_bstream_image_header *catalogue,
                            st_bstream_db_info *db,
                            void *iter)
{
  delete (backup::Image_info::Iterator*)iter;
}


/*****************************************************************

   Services for backup stream library related to meta-data
   manipulation.

 *****************************************************************/

/**
  Create given item using serialization data read from backup image.

  @todo Decide what to do if unknown item type is found. Right now we
  bail out.
 */ 
extern "C"
int bcat_create_item(st_bstream_image_header *catalogue,
                     struct st_bstream_item_info *item,
                     bstream_blob create_stmt,
                     bstream_blob other_meta_data)
{
  using namespace backup;
  using namespace obs;

  DBUG_ASSERT(catalogue);
  DBUG_ASSERT(item);

  Restore_info *info= static_cast<Restore_info*>(catalogue);
  THD *thd= info->m_ctx.thd();
  int create_err= 0;

  switch (item->type) {
  
  case BSTREAM_IT_DB:     create_err= ER_BACKUP_CANT_RESTORE_DB; break;
  case BSTREAM_IT_TABLE:  create_err= ER_BACKUP_CANT_RESTORE_TABLE; break;
  case BSTREAM_IT_VIEW:   create_err= ER_BACKUP_CANT_RESTORE_VIEW; break;
  case BSTREAM_IT_SPROC:  create_err= ER_BACKUP_CANT_RESTORE_SROUT; break;
  case BSTREAM_IT_SFUNC:  create_err= ER_BACKUP_CANT_RESTORE_SROUT; break;
  case BSTREAM_IT_EVENT:  create_err= ER_BACKUP_CANT_RESTORE_EVENT; break;
  case BSTREAM_IT_TRIGGER: create_err= ER_BACKUP_CANT_RESTORE_TRIGGER; break;
  case BSTREAM_IT_TABLESPACE: create_err= ER_BACKUP_CANT_RESTORE_TS; break;
  case BSTREAM_IT_PRIVILEGE: create_err= ER_BACKUP_CANT_RESTORE_PRIV; break;
  
  /*
    TODO: Decide what to do when we come across unknown item:
    break the restore process as it is done now or continue
    with a warning?
  */

  default:
    info->m_ctx.fatal_error(ER_BACKUP_UNKNOWN_OBJECT_TYPE);
    return BSTREAM_ERROR;    
  }

  Image_info::Obj *obj= find_obj(*info, *item);

  if (!obj)
  {
    info->m_ctx.fatal_error(ER_BACKUP_UNKNOWN_OBJECT);
    return BSTREAM_ERROR;
  }

  backup::String sdata(create_stmt.begin, create_stmt.end);

  DBUG_PRINT("restore",("Creating item of type %d pos %ld: %s",
                         item->type, item->pos, sdata.ptr()));
  /*
    Note: The instance created by Image_info::Obj::materialize() is deleted
    when *info is destroyed.
   */ 
  obs::Obj *sobj= obj->materialize(0, sdata);

  Image_info::Obj::describe_buf buf;
  const char *desc= obj->describe(buf);

  if (!sobj)
  {
    info->m_ctx.fatal_error(create_err, desc);
    return BSTREAM_ERROR;
  }

  /*
    If the item we are creating is an event or trigger, we don't execute it
    yet. It will be done in @c Backup_restore_ctx::do_restore() after table
    data has been restored.
   */ 
  
  switch (item->type) {

  case BSTREAM_IT_EVENT:
  case BSTREAM_IT_TRIGGER:
    return BSTREAM_OK;

  default: break;
  
  }

  // If we are to create a tablespace, first check if it already exists.

  if (item->type == BSTREAM_IT_TABLESPACE)
  {
    Obj *ts= obs::find_tablespace(thd, sobj->get_name());

    if (ts)
    {
      /*
        A tablespace with the same name exists. We have to check if other
        attributes are the same as they were.
      */

      if (obs::compare_tablespace_attributes(ts, sobj))
      {
        /* The tablespace is the same. There is nothing more to do. */
        DBUG_PRINT("restore",(" skipping tablespace which exists"));
        return BSTREAM_OK;
      }

      /*
        A tablespace with the same name exists, but it has been changed
        since backup.  We can't re-create the original tablespace used by
        tables being restored. We report this and cancel restore process.
      */

      DBUG_PRINT("restore",
                 (" tablespace has changed on the server - aborting"));
      info->m_ctx.fatal_error(ER_BACKUP_TS_CHANGE, desc);
      delete ts;
      return BSTREAM_ERROR;
    }
  }

  // Create the object.

  /*
    We need to check to see if the user exists (grantee) and if not, 
    do not execute the grant. 
  */
  if (item->type == BSTREAM_IT_PRIVILEGE)
  {
    /*
      Issue warning to the user that grant was skipped. 

      @todo Replace write_message() call with the result of the revised
            error handling work in WL#4384 with possible implementation
            via a related bug report.
    */
    if (!obs::check_user_existence(thd, sobj))
    {
      info->m_ctx.report_error(log_level::WARNING,
                               ER_BACKUP_GRANT_SKIPPED,
                               obs::grant_get_grant_info(sobj)->ptr(),
                               obs::grant_get_user_name(sobj)->ptr());
      return BSTREAM_OK;
    }
    /*
      We need to check the grant against the database list to ensure the
      grants have not been altered to apply to another database.
    */
    ::String db_name;  // db name extracted from grant statement
    char *start;
    char *end;
    int size= 0;

    start= strstr((char *)create_stmt.begin, "ON ") + 3;
    end= strstr(start, ".");
    size= end - start;
    db_name.alloc(size);
    db_name.length(0);
    db_name.append(start, size);
    if (!info->has_db(db_name))
    {
      info->m_ctx.fatal_error(ER_BACKUP_GRANT_WRONG_DB, create_stmt);
      return BSTREAM_ERROR;
    }
  }

  if (sobj->create(thd))
  {
    info->m_ctx.fatal_error(create_err, desc);
    return BSTREAM_ERROR;
  }
  
  return BSTREAM_OK;
}

/**
  Get serialization string for a given object.
  
  The catalogue should contain @c Image_info::Obj instance corresponding to the
  object described by @c item. This instance should contain pointer to 
  @c obs::Obj instance which can be used for getting the serialization string.

  @todo Decide what to do with the serialization string buffer - is it 
  acceptable to re-use a single buffer as it is done now?
 */ 
extern "C"
int bcat_get_item_create_query(st_bstream_image_header *catalogue,
                               struct st_bstream_item_info *item,
                               bstream_blob *stmt)
{
  using namespace backup;
  using namespace obs;

  DBUG_ASSERT(catalogue);
  DBUG_ASSERT(item);
  DBUG_ASSERT(stmt);

  Backup_info *info= static_cast<Backup_info*>(catalogue);

  int meta_err= 0;

  switch (item->type) {
  
  case BSTREAM_IT_DB:     meta_err= ER_BACKUP_GET_META_DB; break;
  case BSTREAM_IT_TABLE:  meta_err= ER_BACKUP_GET_META_TABLE; break;
  case BSTREAM_IT_VIEW:   meta_err= ER_BACKUP_GET_META_VIEW; break;
  case BSTREAM_IT_SPROC:  meta_err= ER_BACKUP_GET_META_SROUT; break;
  case BSTREAM_IT_SFUNC:  meta_err= ER_BACKUP_GET_META_SROUT; break;
  case BSTREAM_IT_EVENT:  meta_err= ER_BACKUP_GET_META_EVENT; break;
  case BSTREAM_IT_TRIGGER: meta_err= ER_BACKUP_GET_META_TRIGGER; break;
  case BSTREAM_IT_TABLESPACE: meta_err= ER_BACKUP_GET_META_TS; break;
  case BSTREAM_IT_PRIVILEGE: meta_err= ER_BACKUP_GET_META_PRIV; break;
  
  /*
    This can't happen - the item was obtained from the backup kernel.
  */
  default: DBUG_ASSERT(FALSE);
  }

  Image_info::Obj *obj= find_obj(*info, *item);

  /*
    The catalogue should contain the specified object and it should have 
    a corresponding server object instance.
   */ 
  DBUG_ASSERT(obj);
  DBUG_ASSERT(obj->m_obj_ptr);
  
  /*
    Note: Using single buffer here means that the string returned by
    this function will live only until the next call. This should be fine
    given the current ussage of the function inside the backup stream library.
    
    TODO: document this or find better solution for string storage.
   */ 
  
  ::String *buf= &(info->serialization_buf);
  buf->length(0);

  if (obj->m_obj_ptr->serialize(info->m_ctx.thd(), buf))
  {
    Image_info::Obj::describe_buf dbuf;

    info->m_ctx.fatal_error(meta_err, obj->describe(dbuf));
    return BSTREAM_ERROR;    
  }

  stmt->begin= (backup::byte*)buf->ptr();
  stmt->end= stmt->begin + buf->length();

  return BSTREAM_OK;
}

/**
  Get extra meta-data (if any) for a given object.
 
  @note Extra meta-data is not used currently.
 */ 
extern "C"
int bcat_get_item_create_data(st_bstream_image_header *catalogue,
                            struct st_bstream_item_info *item,
                            bstream_blob *data)
{
  /* We don't use any extra data now */
  return BSTREAM_ERROR;
}


/*************************************************

           Implementation of Table_ref class

 *************************************************/

namespace backup {

/** 
  Produce string identifying the table in internal format (as used by 
  storage engines).
*/
const char* Table_ref::internal_name(char *buf, size_t len) const
{
  uint plen= build_table_filename(buf, len, 
                                  db().name().ptr(), name().ptr(), 
                                  "", /* no extension */ 
                                  0 /* not a temporary table - do conversions */);
  buf[plen]='\0';
  return buf;    
}

/** 
    Produce human readable string identifying the table 
    (e.g. for error reporting)
*/
const char* Table_ref::describe(char *buf, size_t len) const
{
  my_snprintf(buf, len, "`%s`.`%s`", db().name().ptr(), name().ptr());
  return buf;
}

/*
  TODO: remove these functions. Currently they are only used by the myisam 
  native backup engine.
*/
TABLE_LIST *build_table_list(const Table_list &tables, thr_lock_type lock)
{
  TABLE_LIST *tl= NULL;

  for( uint tno=0; tno < tables.count() ; tno++ )
  {
    TABLE_LIST *ptr = mk_table_list(tables[tno], lock, ::current_thd->mem_root);
    if (!ptr)
    {
      // Failed to allocate (failure has been reported)
      return NULL;
    }
    tl= link_table_list(*ptr,tl);
  }

  return tl;
}

void free_table_list(TABLE_LIST*)
{}

} // backup namespace


/*************************************************

                 Helper functions

 *************************************************/

namespace backup {

/** 
  Build linked @c TABLE_LIST list from a list stored in @c Table_list object.
 
  @note The order of tables in the returned list is different than in the 
  input list (reversed).

  @todo Decide what to do if errors are detected. For example, how to react
  if memory for TABLE_LIST structure could not be allocated?
 */



/*
  The constant is declared here (and memory allocated for it) because
  IBM's xlc compiler requires that. However, the intention was to make it
  a pure symbolic constant (no need to allocate memory). If someone knows
  how to achieve that and keep xlc happy, please let me know. /Rafal
*/ 
const size_t Driver::UNKNOWN_SIZE= static_cast<size_t>(-1);

} // backup namespace
