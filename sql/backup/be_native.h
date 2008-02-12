#ifndef BE_NATIVE_H_
#define BE_NATIVE_H_

#include <backup_engine.h>
#include "../sql_plugin.h"

namespace backup {

/**
  Specialization of @c Image_info for images created by native backup drivers.
 */
class Native_snapshot: public Snapshot_info
{
  const ::handlerton  *m_hton; ///< Pointer to storage engine.
  Engine     *m_be;    ///< Pointer to the native backup engine.
  const char *m_name;  ///< Saved name of storage engine.
  uint       m_se_ver; ///< Storage engine version number.

 public:

  Native_snapshot(const storage_engine_ref se): 
    Snapshot_info(0), m_hton(NULL), m_be(NULL)
  {
    init(se);
    if (m_be)
      m_version= m_be->version();
  }
  
  Native_snapshot(const version_t ver, const storage_engine_ref se): 
    Snapshot_info(ver), m_hton(NULL), m_be(NULL)
  {
    init(se);
  }

  ~Native_snapshot()
  {
    if (m_be)
      m_be->free();
  }

  bool is_valid()
  { return m_be != NULL; }

  enum_snap_type type() const
  { return NATIVE_SNAPSHOT; }

  uint se_ver() const
  { return m_se_ver; }

  const char* se_name() const
  { return m_name; }

  const char* name() const
  { return se_name(); }

  bool accept(const Table_ref&, const storage_engine_ref se)
  { 
    // this assumes handlertons are single instance objects!
    return se_hton(se) == m_hton; 
  }

  result_t get_backup_driver(Backup_driver* &drv)
  {
    DBUG_ASSERT(m_be);
    return m_be->get_backup(Driver::PARTIAL,m_tables,drv);
  }

  result_t get_restore_driver(Restore_driver* &drv)
  {
    DBUG_ASSERT(m_be);
    return m_be->get_restore(m_version,Driver::PARTIAL,m_tables,drv);
  }

 private:

  int init(const storage_engine_ref se);
};

inline
int Native_snapshot::init(const storage_engine_ref se)
{
  m_hton= se_hton(se);
  m_se_ver= se_version(se);

  DBUG_ASSERT(m_hton);
  DBUG_ASSERT(m_hton->get_backup_engine);

  result_t ret= m_hton->get_backup_engine(const_cast<handlerton*>(m_hton), m_be);

  if (ret != OK || !m_be)
  {
    if (m_be)
      m_be->free();
    m_be= NULL;
    return 1;
  }
  
  m_name= ::ha_resolve_storage_engine_name(m_hton);
  return 0;
}


} // backup namespace

#endif /*BE_NATIVE_H_*/
