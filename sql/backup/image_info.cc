#include "../mysql_priv.h"

#include "image_info.h"
#include "be_native.h"

/**
  @file

  @brief Implements @c Image_info class and friends.

  @todo Error reporting
  @todo Store endianess info in the image.
*/

namespace backup {

Image_info::Image_info()
  :data_size(0), m_table_count(0), m_dbs(16, 16)
{
  init_alloc_root(&mem_root, 4 * 1024, 0);

  /* initialize st_bstream_image_header members */

  bzero(static_cast<st_bstream_image_header*>(this),
        sizeof(st_bstream_image_header));

  /* 
    This code reads and writes backup image using version 1 of the backup 
    image format.
   */ 
  version= 1; 

  /*
    The arithmetic below assumes that MYSQL_VERSION_ID digits are arranged
    as follows: HLLRR where
    H - major version number
    L - minor version number
    R - release
  */
  DBUG_PRINT("backup",("version %d", MYSQL_VERSION_ID));
  server_version.major= MYSQL_VERSION_ID / 10000;
  server_version.minor= (MYSQL_VERSION_ID % 10000) / 100;
  server_version.release= MYSQL_VERSION_ID % 100;
  server_version.extra.begin= (byte*)MYSQL_SERVER_VERSION;
  server_version.extra.end= server_version.extra.begin +
                            strlen((const char*)server_version.extra.begin);

  flags= 0;  // TODO: set BSTREAM_FLAG_BIG_ENDIAN flag accordingly

  bzero(m_snap, sizeof(m_snap));
}

Image_info::~Image_info()
{
  // Delete server table objects 
  
  for (uint n=0; n<256; ++n)
  {
    Snapshot_info *snap= m_snap[n];
    
    if (!snap)
      continue;
    
    for (uint i=0; i < snap->table_count(); ++i)
    {
      Table *t= snap->get_table(i);
      
      if (!t)
        continue;
        
      delete t->m_obj_ptr;
    }
  }

  // delete server database objects 

  for (uint i=0; i < db_count(); ++i)
  {
    Db *db= get_db(i);
    
    if (db)
      delete db->m_obj_ptr;
  }

  free_root(&mem_root, MYF(0));
}

Image_info::Db* 
Image_info::add_db(const String &db_name, uint pos)
{
  Db *db= new (&mem_root) Db(db_name);
  
  if (!db)
    return NULL;
  
  if (m_dbs.insert(pos, db))
    return NULL;
  
  db->base.pos= pos;
  
  return db;
}

Image_info::Db* 
Image_info::get_db(uint pos) const
{
  return m_dbs[pos];
}

/**
  Add snapshot to the catalogue.
  
  The snapshot should be non-empty, that is contain data of at least one table.
  Snapshot is added to the list of snapshots used in the image and a number is
  assigned to it. This number is stored in @c snap.m_num. If snapshot's number
  is @c n then pointer to a corresponding @c Snapshot_info object is stored in 
  @c m_snap[n-1].
  
  The @c Snapshot_info object is not owned by @c Image_info instance - it must
  be deleted externally.
 */ 
int Image_info::add_snapshot(Snapshot_info &snap)
{
  uint num= st_bstream_image_header::snap_count++;

  // The limit of 256 snapshots is imposed by backup stream format.  
  if (num > 256)
    return -1;
  
  m_snap[num]= &snap;
  snap.m_num= num + 1;
  
  /* 
    Store information about snapshot in the snapshot[] table for the
    backup stream library
   */

  st_bstream_snapshot_info &info= snapshot[num];
  
  bzero(&info, sizeof(st_bstream_snapshot_info));
  info.type= enum_bstream_snapshot_type(snap.type());
  info.version= snap.version();
  info.table_count= snap.table_count();
  
  if (snap.type() == Snapshot_info::NATIVE_SNAPSHOT)
  {
    Native_snapshot &ns= static_cast<Native_snapshot&>(snap);
    uint se_ver= ns.se_ver();
    const char *se_name= ns.se_name();
    
    info.engine.major= se_ver >> 8;
    info.engine.minor= se_ver & 0xFF;
    info.engine.name.begin= (byte*)se_name;
    info.engine.name.end= info.engine.name.begin + strlen(se_name);    
  }
  
  return num + 1;
}

/**
  Check if catalogue contains given database.
 */ 
bool Image_info::has_db(const String &db_name)
{
  for (uint n=0; n < m_dbs.count() ; ++n)
    if (m_dbs[n] && m_dbs[n]->name() == db_name)
      return TRUE;

  return FALSE;
}

/**
  Add table to the catalogue.
  
  Table's data snapshot is added to the catalogue if it was not there already.
 */
Image_info::Table* 
Image_info::add_table(Db &db, const String &table_name, 
                      Snapshot_info &snap, ulong pos)
{
  Table *t= new (&mem_root) Table(db, table_name);
  
  if (!t)
    return NULL;

  if (snap.add_table(*t, pos))
    return NULL;
  
  if (db.add_table(*t))
    return NULL;

  if (!snap.m_num)
    snap.m_num= add_snapshot(snap); // reports errors

  if (!snap.m_num)
   return NULL;

  t->snap_num= snap.m_num - 1;
  t->base.base.pos= pos;
  t->base.db= &db;

  m_table_count++;

  return t;  
}

Image_info::Table* 
Image_info::get_table(ushort snap_num, ulong pos) const
{
  if (snap_num > snap_count() || m_snap[snap_num] == NULL)
    return NULL;
  
  Table *t= m_snap[snap_num]->get_table(pos);
  
  if (!t)
    return NULL;
  
  return t;
}

/**
  Find object in a catalogue.
  
  The object is identified by its coordinates stored in a 
  @c st_bstream_item_info structure. Normally these coordinates are
  filled by backup stream library when reading backup image.
  
  @returns Pointer to the corresponding @c Obj instance or NULL if object
  was not found.
 */ 
Image_info::Obj *find_obj(const Image_info &info, 
                          const st_bstream_item_info &item)
{
  switch (item.type) {

  case BSTREAM_IT_DB:
    return info.get_db(item.pos);

  case BSTREAM_IT_TABLE:
  {
    const st_bstream_table_info &ti= 
                           reinterpret_cast<const st_bstream_table_info&>(item);

    return info.get_table(ti.snap_num, item.pos);
  }

  default:
    return NULL;
  }
}

} // backup namespace

template class Map<uint, backup::Image_info::Db>;
