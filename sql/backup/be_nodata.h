#ifndef _NODATA_BACKUP_H
#define _NODATA_BACKUP_H

#include <backup_engine.h>
#include <backup/image_info.h>  // to define default backup image class

namespace nodata_backup {

using backup::byte;
using backup::result_t;
using backup::version_t;
using backup::Table_list;
using backup::Table_ref;
using backup::Buffer;

/**
  @class Engine
 
  Encapsulates nodata backup/restore functionality.
 
  Using this class, the caller can create an instance of the nodata backup
  backup and restore class. The nodata driver does not read or write to any 
  files or tables. It is used to allow the backup and restore of storage 
  engines that do not store data.
*/
class Engine: public Backup_engine
{
public:
  /// Constructor
  Engine(THD *t_thd) { m_thd= t_thd; }

  /*
    Return version of backup images created by this engine.
  */
  version_t version() const { return 0; };
  result_t get_backup(const uint32, const Table_list &tables, 
                      Backup_driver* &drv);
  result_t get_restore(const version_t version, const uint32 flags, 
                       const Table_list &tables, Restore_driver* &drv);

  /*
   Free any resources allocated by the nodata backup engine.
  */
  void free() { delete this; }

private:
  THD *m_thd; ///< Current thread reference.
};

/**
  @class Backup
 
  Contains the nodata backup algorithm backup functionality.
 
  Creates a stubbed driver class for the backup kernel code. This
  allows the driver to be used in a backup while not reading data.
*/
class Backup: public Backup_driver
{
public:
  /// Constructor
  Backup(const backup::Table_list &tables):
  Backup_driver(tables) {};
  virtual ~Backup() {}; 
  size_t size()  { return 0; };
  size_t init_size() { return 0; };
  result_t begin(const size_t) { return backup::OK; };
  result_t end() { return backup::OK; };
  result_t get_data(Buffer &buf);
  result_t lock() { return backup::OK; };
  result_t unlock() { return backup::OK; };
  result_t cancel() { return backup::OK; };
  void free() { delete this; };
  result_t prelock() { return backup::OK; };
};

/**
  @class Restore
 
  Contains the nodata backup algorithm restore functionality.
 
  Creates a stubbed driver class for the backup kernel code. This
  allows the driver to be used in a restore while not writing data.
*/
class Restore: public Restore_driver
{
public:
  /// Constructor
  Restore(const Table_list &tables, THD *t_thd):
         Restore_driver(tables) {};
  virtual ~Restore() {};
  result_t begin(const size_t) { return backup::OK; };
  result_t end() { return backup::OK; };
  result_t send_data(Buffer &buf);
  result_t cancel() { return backup::OK; };
  void free() { delete this; };
};
} // nodata_backup namespace


/*********************************************************************

  Nodata snapshot class

 *********************************************************************/

namespace backup {

class Logger;

/**
  @class Nodata_snapshot

  This extends Snapshot_info for implementation of the no data backup driver.
*/
class Nodata_snapshot: public Snapshot_info
{
public:

  /// Constructor
  Nodata_snapshot(Logger&) :Snapshot_info(1) // current version number is 1
  {}
  /// Constructor
  Nodata_snapshot(Logger&, const version_t ver) :Snapshot_info(ver)
  {}

  /// Return snapshot type.
  enum_snap_type type() const
  { return NODATA_SNAPSHOT; }

  /// Return the name of the snapshot.
  const char* name() const
  { return "Nodata"; }

  bool accept(const backup::Table_ref &,const storage_engine_ref e)
  { 
    bool accepted= FALSE;
    const char *ename= se_name(e);

    /*
      Accept only nodata engines.
    */
    if ((my_strcasecmp(system_charset_info, "BLACKHOLE", ename) == 0) ||
        (my_strcasecmp(system_charset_info, "EXAMPLE", ename) == 0) ||
        (my_strcasecmp(system_charset_info, "FEDERATED", ename) == 0) ||
        (my_strcasecmp(system_charset_info, "MRG_MYISAM", ename) == 0))
      accepted= TRUE;
    return (accepted);
  }

  result_t get_backup_driver(Backup_driver* &ptr)
  { return (ptr= new nodata_backup::Backup(m_tables)) ? OK : ERROR; }

  result_t get_restore_driver(Restore_driver* &ptr)
  { return (ptr= new nodata_backup::Restore(m_tables,::current_thd)) ? OK : ERROR; }

  bool is_valid(){ return TRUE; };

};

} // backup namespace


#endif

