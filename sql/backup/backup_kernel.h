#ifndef _BACKUP_KERNEL_API_H
#define _BACKUP_KERNEL_API_H

#include <backup/logger.h>
#include <backup/stream_services.h>

/**
  @file

  Functions and types forming the backup kernel API
*/


/**
  @brief Size of the buffer used for transfers between backup kernel and
  backup/restore drivers.
*/
#define DATA_BUFFER_SIZE  (1024*1024)

/*
  Functions used to initialize and shut down the online backup system.
  
  Note: these functions are called at plugin load and plugin shutdown time,
  respectively.
 */ 
int backup_init();
void backup_shutdown();

/*
  Called from the big switch in mysql_execute_command() to execute
  backup related statement
*/
int execute_backup_command(THD*, LEX*, String*, bool);

// forward declarations

class Backup_info;
class Restore_info;

namespace backup {

class Mem_allocator;
class Stream;
class Output_stream;
class Input_stream;
class Native_snapshot;

int write_table_data(THD*, Backup_info&, Output_stream&);
int restore_table_data(THD*, Restore_info&, Input_stream&);

}

/**
  Instances of this class are used for creating required context and performing
  backup/restore operations.
  
  @see kernel.cc
 */ 
class Backup_restore_ctx: public backup::Logger 
{
 public:

  Backup_restore_ctx(THD*);
  ~Backup_restore_ctx();

  bool is_valid() const;
  ulonglong op_id() const;

  Backup_info*  prepare_for_backup(String *location, 
                                   LEX_STRING orig_loc, 
                                   const char*, bool);
  Restore_info* prepare_for_restore(String *location, 
                                   LEX_STRING orig_loc,
                                   const char*);  

  int do_backup();
  int do_restore(bool overwrite);

  int close();

  THD* thd() const { return m_thd; }

 private:

  // Prevent copying/assignments
  Backup_restore_ctx(const Backup_restore_ctx&);
  Backup_restore_ctx& operator=(const Backup_restore_ctx&);

  /** @c current_op points to the @c Backup_restore_ctx for the
      ongoing backup/restore operation.  If pointer is null, no
      operation is currently running. */
  static Backup_restore_ctx *current_op;
  /**
     Indicates if @c run_lock mutex was initialized and thus it should
     be properly destroyed during shutdown. @sa backup_shutdown().
   */
  static bool run_lock_initialized;
  static pthread_mutex_t  run_lock; ///< To guard @c current_op.

  /** 
    @brief State of a context object. 
    
    Backup/restore can be performed only if object is prepared for that 
    operation.
   */
  enum { CREATED,
         PREPARED_FOR_BACKUP,
         PREPARED_FOR_RESTORE,
         CLOSED } m_state;

  ulonglong m_thd_options;  ///< For saving thd->options.
  /**
    @brief Tells if context object is in error state.

    In case of fatal error, the context object is put into an error state 
    by setting @m_error to non-zero value. This can be the code of
    the detected error but currently the exact value is not used.

    When in error state, public methods of Backup_restore_ctx do not try
    to perform their operations but report an error instead. @c Is_valid() 
    will return FALSE for an object in error state.

    @note The error state is an internal state of the context object. The
    object can enter this state only as a result of executing one of its 
    methods.
  */
  int m_error;
  int fatal_error(int);
  
  ::String  m_path;   ///< Path to where the backup image file is located.

  /** If true, the backup image file is deleted at clean-up time. */
  bool m_remove_loc;

  backup::Stream *m_stream; ///< Pointer to the backup stream object, if opened.
  backup::Image_info *m_catalog;  ///< Pointer to the image catalogue object.

  /** Memory allocator for backup stream library. */
  backup::Mem_allocator *mem_alloc;

  int prepare_path(::String *backupdir, 
                   LEX_STRING orig_loc);
  int prepare(::String *backupdir, LEX_STRING location);
  void disable_fkey_constraints();
  int  restore_triggers_and_events();
  
  /** 
    Indicates if tables have been locked with @c lock_tables_for_restore()
  */
  bool m_tables_locked; 

  /**
    Indicates we must turn binlog back on in the close method. This is
    set to TRUE in the prepare_for_restore() method.
  */
  bool m_engage_binlog;

  int lock_tables_for_restore();
  void unlock_tables();
  
  int report_stream_open_failure(int open_error, const LEX_STRING *location);

  friend int backup_init();
  friend void backup_shutdown();
  friend bstream_byte* bstream_alloc(unsigned long int);
  friend void bstream_free(bstream_byte *ptr);
};

/// Check if instance is correctly created.
inline
bool Backup_restore_ctx::is_valid() const
{
  return m_error == 0;
}

/// Return global id of the backup/restore operation.
inline
ulonglong Backup_restore_ctx::op_id() const
{
  return get_op_id(); // inherited from Logger class
}

/// Disable foreign key constraint checks (needed during restore).
inline
void Backup_restore_ctx::disable_fkey_constraints()
{
  m_thd->options|= OPTION_NO_FOREIGN_KEY_CHECKS;
}

/**
  Move context object into error state.
  
  After this method is called the context object is in error state and
  cannot be normally used. The provided error code is saved in m_error 
  member.
  
  Only one fatal error can be reported. If context is already in error
  state when this method is called, it does nothing.

  @note Context object should enter error state only as a result of executing
  one of its methods. Thus this private helper method is intended to be used 
  only from within Backup_restore_ctx class.  
  
  @return error code given as input or stored in the context object if
  it is already in error state.
 */ 
inline
int Backup_restore_ctx::fatal_error(int error_code)
{
  m_remove_loc= TRUE;

  if (m_error)
    return m_error;

  m_error= error_code;

  return error_code;
}

/*
  Now, when Backup_restore_ctx is defined, include definitions
  of Backup_info and Restore_info classes.
*/

#include <backup/backup_info.h>
#include <backup/restore_info.h>

#endif
