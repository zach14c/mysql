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
 * @class Backup
 *
 * @brief Contains the snapshot backup algorithm backup functionality.
 *
 * The backup class is a row-level backup mechanism designed to perform
 * a table scan on each table reading the rows and saving the data to the
 * buffer from the backup algorithm using a consistent read transaction.
 *
 * @see <backup driver>
 */
class Backup: public default_backup::Backup
{
  public:
    Backup(const Table_list &tables, THD *t_thd) 
      :default_backup::Backup(tables, t_thd, TL_READ) 
    { 
      tables_open= FALSE;
      m_cancel= FALSE;
      m_trans_start= FALSE;
    };
    virtual ~Backup() { cleanup(); };
    result_t begin(const size_t) { return backup::OK; };
    result_t end() { return backup::OK; };
    result_t get_data(Buffer &buf);
    result_t prelock() { return backup::READY; }
    result_t lock();
    result_t unlock() { return backup::OK; };
    result_t cancel() 
    { 
      m_cancel= TRUE;
      cleanup();
      return backup::OK;
    }
  private:
    my_bool tables_open;   ///< Indicates if tables are open
    my_bool m_cancel;      ///< Cancel backup
    my_bool m_trans_start; ///< Is transaction stated?

   result_t cleanup();
};

/**
 * @class Restore
 *
 * @brief Contains the snapshot backup algorithm restore functionality.
 *
 * The restore class is a row-level backup mechanism designed to restore
 * data for each table by writing the data for the rows from the
 * buffer given by the backup algorithm.
 *
 * @see <restore driver>
 */
class Restore: public default_backup::Restore
{
  public:
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


class CS_snapshot: public Logical_snapshot
{
 public:

  CS_snapshot(Logger&) :Logical_snapshot(1) // current version number is 1
  {}
  CS_snapshot(Logger&, version_t ver) :Logical_snapshot(ver)
  {}

  enum_snap_type type() const
  { return CS_SNAPSHOT; }

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

