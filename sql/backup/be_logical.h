#ifndef _BE_LOGICAL_H_
#define _BE_LOGICAL_H_

/** 
  @file

  This header contains definitions needed for "logical" backup/restore 
  drivers which access tables using handlerton interface. The built-in
  drivers are of this type.
*/ 

#include <backup/image_info.h> // For definition of Snapshot_info

class Backup_restore_ctx;

namespace backup {

/**
  Extends Snapshot_info with methods for accessing tables opened in the server.
*/
class Logical_snapshot :public Snapshot_info
{
public:

  Logical_snapshot(version_t ver) :Snapshot_info(ver) {}
  TABLE*      get_opened_table(ulong pos) const;
  const Table_list& get_table_list() const;
};

/**
  Get opened TABLE structure for the table at position @c pos.

  This method should be called only after tables have been opened and locked
  by the backup kernel.
*/ 
inline
TABLE *Logical_snapshot::get_opened_table(ulong pos) const
{
  Image_info::Table *t= get_table(pos);

  // If we have table at pos, then the m_table pointer should be set for it.
  DBUG_ASSERT(!t || t->m_table);

  return t ? t->m_table->table : NULL;
}

inline
const Table_list& Logical_snapshot::get_table_list() const
{
  return m_tables;
}

} // backup namespace


#endif /*BE_LOGICAL_H_*/
