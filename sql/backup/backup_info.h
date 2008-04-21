#ifndef BACKUP_INFO_H_

#include <backup/image_info.h>
#include <backup/stream_services.h>

class Backup_restore_ctx;
class Backup_info;

namespace backup {

class Native_snapshot;
class Logger;
class Output_stream;

int write_table_data(THD*, backup::Logger&, Backup_info&, 
                     backup::Output_stream&);

} // backup namespace

/**
  Specialization of @c Image_info which adds methods for selecting objects
  to backup.

  A pointer to empty @c Backup_info instance is returned by 
  @c Backup_restore_ctx::prepare_for_backup() method. Methods @c add_dbs() or 
  @c add_all_dbs() can be used to select which databases should be backed up. 
  When this is done, the @c Backup_info object should be "closed" with 
  @c close() method. Only then we are ready for backup operation.
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

  Ts* add_ts(obs::Obj*);
  Db* add_db(obs::Obj*);
  Dbobj* add_db_object(Db&, const obj_type, obs::Obj*);
  Table* add_table(Db&, obs::Obj*);

  int add_db_items(Db&);
  int add_objects(Db&, const obj_type, obs::ObjIterator&);
  
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
  Map<storage_engine_ref, backup::Native_snapshot > native_snapshots;

  struct Ts_hash_node;	///< Hash nodes used in @c ts_hash.

  /**
    Hash storing all tablespaces added to the backup catalogue.
    
    Used for quickly determining if the catalogue contains a given
    tablespace or not.
   */ 
  HASH   ts_hash;

  String serialization_buf; ///< Used to store serialization strings of objects.
  
  friend int backup::write_table_data(THD*, backup::Logger&, Backup_info&, 
                                      backup::Output_stream&);
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

#define BACKUP_INFO_H_

#endif /*BACKUP_INFO_H_*/
