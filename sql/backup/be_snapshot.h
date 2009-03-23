#ifndef _SNAPSHOT_BACKUP_H
#define _SNAPSHOT_BACKUP_H

#include <backup/image_info.h>        
#include <backup/buffer_iterator.h>
#include <backup/be_default.h>

namespace snapshot_backup {

using backup::byte;
using backup::result_t;
using backup::version_t;
using backup::Table_list;
using backup::Table_ref;
using backup::Buffer;

/**
  @class Backup
 
  Contains the snapshot backup algorithm backup functionality.
 
  The backup class is a row-level backup mechanism designed to perform
  a table scan on each table reading the rows and saving the data to the
  buffer from the backup algorithm using a consistent read transaction.
 
  @see Backup_driver
 */
class Backup: public default_backup::Backup
{
public:
    /// Constructor
    Backup(const Table_list &tables, THD *t_thd) 
      :default_backup::Backup(tables, t_thd, TL_READ) 
    { 
      tables_open= FALSE;
      m_cancel= FALSE;
      m_trans_start= FALSE;
    };
    /// Destructor
    virtual ~Backup() { cleanup(); };

    /// Initialize backup process
    result_t begin(const size_t) { return backup::OK; }; 
    /// End backup process
    result_t end() { return backup::OK; }
    result_t get_data(Buffer &buf);
    /// Initiate a prelock
    result_t prelock() { return backup::READY; }
    result_t lock();
    /// Unlock signal
    result_t unlock() { return backup::OK; };
    /// Cancel the process
    result_t cancel() 
    { 
      m_cancel= TRUE;
      cleanup();
      DBUG_EXECUTE_IF("backup_driver_cancel_error", return backup::ERROR;);
      return backup::OK;
    }
private:
    my_bool tables_open;   ///< Indicates if tables are open
    my_bool m_cancel;      ///< Cancel backup
    my_bool m_trans_start; ///< Is transaction stated?

   result_t cleanup();
};

/**
  @class Restore
 
  Contains the snapshot backup algorithm restore functionality.
 
  The restore class is a row-level backup mechanism designed to restore
  data for each table by writing the data for the rows from the
  buffer given by the backup algorithm.
 
  @see Restore_driver
 */
class Restore: public default_backup::Restore
{
public:
    /// Constructor
    Restore(const backup::Logical_snapshot &snap, THD *t_thd)
      :default_backup::Restore(snap, t_thd){};
    virtual ~Restore(){};
    void free() { delete this; };
};
} // snapshot_backup namespace


/*********************************************************************

  Snapshot image class

 *********************************************************************/

namespace backup {

/**
  Extends Logical_info to implement the consistent snapshot backup driver.
*/
class CS_snapshot: public Logical_snapshot
{
public:

  /// Constructor
  CS_snapshot(Logger&) :Logical_snapshot(1) // current version number is 1
  {}
  /// Constructor
  CS_snapshot(Logger&, version_t ver) :Logical_snapshot(ver)
  {}

  /// Return snapshot type.
  enum_snap_type type() const
  { return CS_SNAPSHOT; }

  /// Return the name of the snapshot.
  const char* name() const
  { return "Snapshot"; }

  bool accept(const Table_ref&, const storage_engine_ref se)
  {
    ::handlerton *h= se_hton(se);

    return (h->start_consistent_snapshot != NULL);
  }; // accept all tables that support consistent read

  result_t get_backup_driver(Backup_driver* &ptr)
  { return (ptr= new snapshot_backup::Backup(m_tables, ::current_thd)) ? OK : ERROR; }

  result_t get_restore_driver(Restore_driver* &ptr)
  { return (ptr= new snapshot_backup::Restore(*this, ::current_thd)) ? OK : ERROR; }

  bool is_valid(){ return TRUE; };

};

} // backup namespace


#endif

