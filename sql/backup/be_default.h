#ifndef _DEFAULT_BACKUP_H
#define _DEFAULT_BACKUP_H

#include <backup_engine.h>
#include <backup/image_info.h>  // to define default backup image class
#include <backup/be_logical.h>
#include <backup/buffer_iterator.h>
#include <backup/be_thread.h>

namespace default_backup {

using backup::byte;
using backup::result_t;
using backup::version_t;
using backup::Table_list;
using backup::Table_ref;
using backup::Buffer;

const size_t META_SIZE= 1;

/*
  The following are the flags for the first byte in the data layout for
  the default and consistent snapshot algorithms. They describe what is 
  included in the buffer going to the kernel.
*/
const byte RCD_ONCE=    1U;     // Single data block for record data
const byte RCD_FIRST=  (1U<<1); // First data block in buffer for record buffer
const byte RCD_DATA=   (1U<<2); // Intermediate data block for record buffer
const byte RCD_LAST=   (1U<<3); // Last data block in buffer for record buffer
const byte BLOB_ONCE=   3U;     // Single data block for blob data
const byte BLOB_FIRST= (3U<<1); // First data block in buffer for blob buffer
const byte BLOB_DATA=  (3U<<2); // Intermediate data block for blob buffer
const byte BLOB_LAST=  (3U<<3); // Last data block in buffer for blob buffer


/**
 * @class Backup
 *
 * @brief Contains the default backup algorithm backup functionality.
 *
 * The backup class is a row-level backup mechanism designed to perform
 * a table scan on each table reading the rows and saving the data to the
 * buffer from the backup algorithm.
 *
 * @see <backup driver> and <backup thread driver>
 */
class Backup: public Backup_thread_driver
{
  public:
    enum has_data_info { YES, WAIT, EOD };
    Backup(const Table_list &tables, THD *t_thd, thr_lock_type lock_type);
    virtual ~Backup(); 
    size_t size()  { return UNKNOWN_SIZE; };
    size_t init_size() { return 0; };
    result_t  begin(const size_t) { return backup::OK; };
    result_t end() { return backup::OK; };
    result_t get_data(Buffer &buf);
    result_t lock() { return backup::OK; };
    result_t unlock() { return backup::OK; };
    result_t cancel() 
    { 
      mode= CANCEL;
      cleanup();
      return backup::OK;
    }
    TABLE_LIST *get_table_list() { return all_tables; }
    void free() { delete this; };
    result_t prelock(); 

 protected:
    TABLE *cur_table;              ///< The table currently being read.
    my_bool init_phase_complete;   ///< Used to identify end of init phase.
    my_bool locks_acquired;        ///< Used to help kernel synchronize drivers.
    handler *hdl;                  ///< Pointer to table handler.
    my_bool m_cleanup;             ///< Is call to cleanup() needed?
    result_t end_tbl_read(); 

  private:
    /*
      We use an enum to control the flow of the algorithm. Each mode 
      invokes a different behavior through a large switch. The mode is
      set in the code as a response to conditions or flow of data.
    */
    typedef enum {
      INITIALIZE,                  ///< Indicates time to initialize read
      CANCEL,                      ///< Indicates time to cancel operation
      GET_NEXT_TABLE,              ///< Open next table in the list
      READ_RCD,                    ///< Reading rows from table mode
      READ_RCD_BUFFER,             ///< Buffer records mode
      CHECK_BLOBS,                 ///< See if record has blobs
      READ_BLOB,                   ///< Reading blobs from record mode
      READ_BLOB_BUFFER             ///< Buffer blobs mode
    } BACKUP_MODE;

    result_t start_tbl_read(TABLE *tbl);
    int next_table();
    BACKUP_MODE mode;              ///< Indicates which mode the code is in
    ulong tbl_num;                   ///< The index of the current table.
    uint *cur_blob;                ///< The current blob field.
    uint *last_blob_ptr;           ///< Position of last blob field.
    MY_BITMAP *read_set;           ///< The file read set.
    Buffer_iterator rec_buffer;    ///< Buffer iterator for windowing records
    Buffer_iterator blob_buffer;   ///< Buffer iterator for windowing BLOB fields
    byte *ptr;                     ///< Pointer to blob data from record.
    TABLE_LIST *all_tables;        ///< Reference to list of tables used.

    result_t cleanup();
    uint pack(byte *rcd, byte *packed_row);
};

/**
 * @class Restore
 *
 * @brief Contains the default backup algorithm restore functionality.
 *
 * The restore class is a row-level backup mechanism designed to restore
 * data for each table by writing the data for the rows from the
 * buffer given by the backup algorithm.
 *
 * @see <restore driver>
 */
class Restore: public Restore_driver
{
  public:
    enum has_data_info { YES, WAIT, EOD };
    Restore(const backup::Logical_snapshot &info, THD *t_thd);
    virtual ~Restore()
    { 
      cleanup();
    }; 
    result_t  begin(const size_t) { return backup::OK; };
    result_t  end();
    result_t  send_data(Buffer &buf);
    result_t  cancel()
    { 
      mode= CANCEL;
      cleanup();
      return backup::OK;
    }
    void free() { delete this; };

 private:
     /*
      We use an enum to control the flow of the algorithm. Each mode 
      invokes a different behavior through a large switch. The mode is
      set in the code as a response to conditions or flow of data.
    */
    typedef enum {
      INITIALIZE,                  ///< Indicates time to initialize read
      CANCEL,                      ///< Indicates time to cancel operation
      GET_NEXT_TABLE,              ///< Open next table in the list
      WRITE_RCD,                   ///< Writing rows from table mode
      CHECK_BLOBS,                 ///< See if record has blobs
      WRITE_BLOB,                  ///< Writing blobs from record mode
      WRITE_BLOB_BUFFER            ///< Buffer blobs mode
    } RESTORE_MODE;

    /**
      Reference to the corresponding logical snapshot object.
    */
    const backup::Logical_snapshot &m_snap;  
    RESTORE_MODE mode;             ///< Indicates which mode the code is in
    uint tbl_num;                  ///< The index of the current table.
    uint32 max_blob_size;          ///< The total size (sum of parts) for the blob.
    TABLE *cur_table;              ///< The table currently being read.
    handler *hdl;                  ///< Pointer to table handler.
    uint *cur_blob;                ///< The current blob field.
    uint *last_blob_ptr;           ///< Position of last blob field.
    Buffer_iterator rec_buffer;    ///< Buffer iterator for windowing records
    Buffer_iterator blob_buffer;   ///< Buffer iterator for windowing BLOB fields
    byte *blob_ptrs[MAX_FIELDS];   ///< List of blob pointers used
    int blob_ptr_index;            ///< Position in blob pointer list
    THD *m_thd;                    ///< Pointer to current thread struct.
    timestamp_auto_set_type old_tm;///< Save old timestamp auto set type.
    my_bool m_cleanup;             ///< Is call to cleanup() needed?

    result_t cleanup();
    uint unpack(byte *packed_row);
};
} // default_backup namespace


/*********************************************************************

  Default snapshot class

 *********************************************************************/

namespace backup {

class Logger;

class Default_snapshot: public Logical_snapshot
{
 public:

  Default_snapshot(Logger&) :Logical_snapshot(1) // current version number is 1
  {}
  Default_snapshot(Logger&, const version_t ver) :Logical_snapshot(ver)
  {}

  enum_snap_type type() const
  { return DEFAULT_SNAPSHOT; }

  const char* name() const
  { return "Default"; }

  bool accept(const backup::Table_ref &,const storage_engine_ref e)
  { 
    bool accepted= TRUE;
    const char *ename= se_name(e);

    /*
      Do not accept nodata engines.
    */
    if ((my_strcasecmp(system_charset_info, "BLACKHOLE", ename) == 0) ||
        (my_strcasecmp(system_charset_info, "EXAMPLE", ename) == 0) ||
        (my_strcasecmp(system_charset_info, "FEDERATED", ename) == 0) ||
        (my_strcasecmp(system_charset_info, "MRG_MYISAM", ename) == 0))
      accepted= FALSE;
    return (accepted);
  }

  result_t get_backup_driver(Backup_driver* &ptr)
  { return (ptr= new default_backup::Backup(m_tables, ::current_thd,
                                            TL_READ_NO_INSERT)) ? OK : ERROR; }

  result_t get_restore_driver(Restore_driver* &ptr)
  { return (ptr= new default_backup::Restore(*this, ::current_thd)) ? OK : ERROR; }

  bool is_valid(){ return TRUE; };
  
};

} // backup namespace


#endif

