#ifndef _BACKUP_KERNEL_API_H
#define _BACKUP_KERNEL_API_H

#include <backup/api_types.h>
#include <backup/catalog.h>
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
int execute_backup_command(THD*, LEX*);

// forward declarations

class Backup_info;
class Restore_info;

namespace backup {

class Mem_allocator;
class OStream;
class IStream;
class Native_snapshot;

int write_table_data(THD*, Logger&, Backup_info&, OStream&);
int restore_table_data(THD*, Logger&, Restore_info&, IStream&);

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
  ulong op_id() const;

  Backup_info*  prepare_for_backup(LEX_STRING location);
  Restore_info* prepare_for_restore(LEX_STRING location);  

  int do_backup();
  int do_restore();

  int close();

  THD *m_thd;

 private:

  static bool is_running; ///< Indicates if a backup/restore operation is in progress.
  static pthread_mutex_t  run_lock; ///< To guard @c is_running flag.

  /** 
    @brief State of a context object. 
    
    Backup/restore can be performed only if object is prepared for that operation.
   */
  enum { CREATED,
         PREPARED_FOR_BACKUP,
         PREPARED_FOR_RESTORE,
         CLOSED } m_state;

  ulong m_thd_options;  ///< For saving thd->options.
  /**
    If backup/restore was interrupted by an error, this member stores the error 
    number.
   */ 
  int m_error;
  
  const char *m_path;   ///< Path to the file where backup image is located.
  bool m_remove_loc;    ///< If true, the backup image file is deleted at clean-up time.
  backup::Stream *m_stream; ///< Pointer to the backup stream object if it is opened.
  backup::Image_info *m_catalog;  ///< Pointer to the image catalogue object.
  static backup::Mem_allocator *mem_alloc; ///< Memory allocator for backup stream library.

  int prepare(LEX_STRING location);
  void disable_fkey_constraints();
  
  friend class Backup_info;
  friend class Restore_info;
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
ulong Backup_restore_ctx::op_id() const
{
  return m_op_id; // inherited from Logger class
}

/// Disable foreign key constraint checks (needed during restore).
inline
void Backup_restore_ctx::disable_fkey_constraints()
{
  m_thd->options|= OPTION_NO_FOREIGN_KEY_CHECKS;
}

/**
  Specialization of @c Image_info which adds methods for selecting objects
  to backup.

  A pointer to empty @c Backup_info instance is returned by 
  @c Backup_restore_ctx::prepare_for_backup() method.
  Methods @c add_dbs() or @c add_all_dbs() can be used to select which databases
  should be backed up. When this is done, the @c Backup_info object should be
  "closed" with @c close() method. Only then it is ready to be used in a backup
  operation.
*/
class Backup_info: public backup::Image_info
{
 public:

  Backup_restore_ctx &m_ctx;

  Backup_info(Backup_restore_ctx&);
  ~Backup_info();

  bool is_valid();

  int add_dbs(List< ::LEX_STRING >&);
  int add_all_dbs();

  int close();

 private:

  /// State of the info structure.
  enum {CREATED,
        CLOSED,  
        ERROR}   m_state;

  backup::Snapshot_info* find_backup_engine(const backup::Table_ref&);

  Db* add_db(obs::Obj*);
  Table* add_table(Db&, obs::Obj*);

  int add_db_items(Db&);
  
  /**
    List of existing @c Snapshot_info objects.
    
    This list is used when selecting a backup engine for a given table. Order
    of this list is important -- more preferred snapshots should come first. 
   */ 
  List<backup::Snapshot_info> snapshots;
  
  /**
    Stores existing native snapshot objects.
    
    Given reference to a storage engine, a corresponding native snapshot object
    can be quickly located if it was already created. 
   */ 
  Map<storage_engine_ref,backup::Native_snapshot > native_snapshots;

  String serialization_buf; ///< Used to store serialization strings of backed-up objects.
  
  friend int backup::write_table_data(THD*, backup::Logger&, Backup_info&, 
                                      backup::OStream&);
  // Needs access to serialization_buf
  friend int ::bcat_get_item_create_query(st_bstream_image_header *catalogue,
                               struct st_bstream_item_info *item,
                               bstream_blob *stmt);
};

/// Check if instance is correctly created.
inline
bool Backup_info::is_valid()
{
  return m_state != ERROR;
}

/**
  Close @c Backup_info object after populating it with items.

  After this call the @c Backup_info object is ready for use as a catalogue
  for backup stream functions such as @c bstream_wr_preamble().
 */
inline
int Backup_info::close()
{
  if (!is_valid())
    return ERROR;

  if (m_state == CLOSED)
    return 0;

  // report backup drivers used in the image
  
  for (uint no=0; no < snap_count(); ++no)
    m_ctx.report_driver(m_snap[no]->name());
  
  m_state= CLOSED;
  return 0;
}  


/**
  Specialization of @c Image_info which is in restore operation.

  An instance of this class is created by 
  @c Backup_restore_ctx::prepare_for_restore() method, which fills the catalogue
  with data read from a backup image.
  
  Currently it is not possible to select objects which will be restored. Thus
  this class can only be used to examine what is going to be restored.
 */

class Restore_info: public backup::Image_info
{
 public:

  Backup_restore_ctx &m_ctx;

  Restore_info(Backup_restore_ctx&);
  ~Restore_info();

  bool is_valid() const;

 private:

  friend int backup::restore_table_data(THD*, backup::Logger&, Restore_info&, 
                                        backup::IStream&);
  friend int ::bcat_add_item(st_bstream_image_header*,
                             struct st_bstream_item_info*);
};

inline
Restore_info::Restore_info(Backup_restore_ctx &ctx):
  m_ctx(ctx)
{}

inline
Restore_info::~Restore_info()
{
  /*
    Delete Snapshot_info instances - they are created in bcat_reset(). 
   */
  for (uint no=0; no < snap_count(); ++no)
    delete m_snap[no];
}

inline
bool Restore_info::is_valid() const
{
  return TRUE; 
}

#endif
