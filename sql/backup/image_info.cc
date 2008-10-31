#include "../mysql_priv.h"

#include "image_info.h"
#include "be_native.h"

/**
  @file

  @brief Implements @c Image_info class and friends.

*/

namespace backup {

Image_info::Image_info()
  :data_size(0), m_table_count(0), m_dbs(16, 16), m_ts_map(16,16)
{
  init_alloc_root(&mem_root, 4 * 1024, 0);      // Never errors

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

  flags= 0;

#ifdef WORDS_BIGENDIAN
  flags|= BSTREAM_FLAG_BIG_ENDIAN;
#endif

  bzero(m_snap, sizeof(m_snap));
}

Image_info::~Image_info()
{
  /* 
    We need to explicitly call destructors for all objects in the catalogue
    since they are allocated using mem_root and thus destructors will not be
    invoked when the mem_root is freed.
  */

  // first tablespaces

  Ts_iterator tsit(*this);
  Ts *ts;

  while ((ts= static_cast<Ts*>(tsit++)))
    ts->~Ts();

  Db_iterator dbit(*this);
  Db *db;

  // then databases and all objects inside each database

  while ((db= static_cast<Db*>(dbit++)))
  {
    // iterate over objects in the database

    Dbobj_iterator it(*this,*db);
    Obj *o;

    while ((o= it++))
      o->~Obj();

    db->~Db(); 
  }

  free_root(&mem_root, MYF(0)); 
}

/**
  Add database to the catalogue.

  @param[in] db_name  name of the database
  @param[in] pos      position at which this database should be stored

  @returns Pointer to @c Image_info::Db instance storing information 
  about the database or NULL in case of error.

  @see @c get_db().
 */
Image_info::Db* 
Image_info::add_db(const String &db_name, uint pos)
{
  Db *db= new (&mem_root) Db(db_name);
  
  if (!db)
    return NULL;
  
  // call destructor if position is occupied
  if (m_dbs[pos])
    m_dbs[pos]->~Db();

  if (m_dbs.insert(pos, db))
    return NULL;
  
  db->base.pos= pos;
  
  return db;
}

/**
  Add tablespace to the catalogue.

  @param[in] ts_name  name of the tablespace
  @param[in] pos      position at which this database should be stored

  @returns Pointer to @c Image_info::Ts instance storing information 
  about the tablespace or NULL in case of error.

  @see @c get_ts().
 */
Image_info::Ts* 
Image_info::add_ts(const String &ts_name, uint pos)
{
  Ts *ts= new (&mem_root) Ts(ts_name);
  
  if (!ts)
    return NULL;
  
  // call destructor if position is occupied
  if (m_ts_map[pos])
    m_ts_map[pos]->~Ts();

  if (m_ts_map.insert(pos, ts))
    return NULL;
  
  ts->base.pos= pos;
  
  return ts;
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

  @returns Snapshot's number or -1 in case of error.
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
  Add per database object to the catalogue.

  @param[in]  db  database to which this object belongs - this database must
                  already be in the catalogue
  @param[in] type type of the object
  @param[in] name name of the object
  @param[in] pos  position where the object will be stored inside database's
                  object list

  @returns Pointer to @c Image_info::Dbobj instance storing information 
  about the object or NULL in case of error.

  @note There is a specialized method @c add_table() for adding tables.

  @see @c get_db_object().
 */
Image_info::Dbobj* Image_info::add_db_object(Db &db,
                                             const enum_bstream_item_type type,
                                             const ::String &name, ulong pos)
{
  Dbobj *o= new (&mem_root) Dbobj(db, type, name);

  if (!o)
    return NULL;  
  
  if (db.add_obj(*o, pos))
    return NULL;
    
  o->base.pos= pos;

  return o;
}

/**
  Return per database object stored in catalogue.

  This method is used only for non-table objects.

  @param[in]  db_num  position of object's database in the catalogue 
  @param[in]  pos     position of the object inside the database

  @returns Pointer to @c Image_info::Dbobj instance storing information 
  about the object or NULL if there is no object with given coordinates.
 */
Image_info::Dbobj* Image_info::get_db_object(uint db_num, ulong pos) const
{
  Db *db= get_db(db_num);

  if (!db)
    return NULL;

  return db->get_obj(pos);
}

/**
  Add table to the catalogue.

  @param[in]  db  table's database - this database must already be in 
                  the catalogue
  @param[in] name name of the table
  @param[in] snap snapshot containing table's data
  @param[in] pos  table's position within the snapshot

  @returns Pointer to @c Image_info::Table instance storing information 
  about the table or NULL in case of error.

  @note The snapshot is added to the catalogue if it was not there already.

  @see @c get_table().
 */
Image_info::Table* 
Image_info::add_table(Db &db, const ::String &table_name, 
                      Snapshot_info &snap, ulong pos)
{
  Table *t= new (&mem_root) Table(db, table_name);
  
  if (!t)
    return NULL;

  if (snap.add_table(*t, pos))  // reports errors
    return NULL;

  db.add_table(*t);                             // Never errors

  if (!snap.m_num)
    snap.m_num= add_snapshot(snap); // reports errors

  if (!snap.m_num)
   return NULL;

  t->snap_num= snap.m_num - 1;
  t->base.base.pos= pos;

  m_table_count++;

  return t;  
}

/**
  Return table stored in the catalogue.

  @param[in] snap_num position of table's snapshot within the catalogue
  @param[in] pos      position of the table within the snapshot

  @returns Pointer to @c Image_info::Table instance storing information 
  about the table or NULL if there is no table with given coordinates.
 */ 
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
  Find object in the catalogue.
  
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

  case BSTREAM_IT_TABLESPACE:
    return info.get_ts(item.pos);

  case BSTREAM_IT_DB:
    return info.get_db(item.pos);

  case BSTREAM_IT_TABLE:
  {
    const st_bstream_table_info &ti= 
                           reinterpret_cast<const st_bstream_table_info&>(item);

    return info.get_table(ti.snap_num, item.pos);
  }

  case BSTREAM_IT_VIEW:
  case BSTREAM_IT_SPROC:
  case BSTREAM_IT_SFUNC:
  case BSTREAM_IT_EVENT:
  case BSTREAM_IT_TRIGGER:
  case BSTREAM_IT_PRIVILEGE:
  {
    const st_bstream_dbitem_info &it=
                          reinterpret_cast<const st_bstream_dbitem_info&>(item);

    return info.get_db_object(it.db->base.pos, item.pos);
  }

  default:
    return NULL;
  }
}

} // backup namespace

template class Map<uint, backup::Image_info::Db>;
