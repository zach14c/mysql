#ifndef CATALOG_H_
#define CATALOG_H_

#include <si_objects.h>
#include <backup_stream.h> // for st_bstream_* types
#include <backup/stream.h> // for I/OStream classes
#include <backup/backup_aux.h>

namespace backup {

/********************************************************************
 
   Image_info and Snapshot_info classes.
 
 ********************************************************************/ 


class Snapshot_info;

/**
  Describes contents of a backup image.

  This class stores a catalogue of a backup image, that is, description of
  all objects stored in it (currently only databases and tables).

  Only object names are stored in the catalogue. Other object data is stored
  in the meta-data part of the image and in case of tables, their data is
  stored in table data snapshots created by backup drivers.

  For each snapshot present in the image there is a @c Snapshot_info object.
  A pointer to n-th snapshot object is stored in @c m_snap[no]. This object 
  contains list of tables whose data is stored in it. Note that each table must 
  belong to exactly one snapshot.

  Each object in the catalogue has its coordinates. The format of these 
  coordinates depends on the object type. For databases, it is just its number. 
  For tables, its coordinates are the number of the snapshot to which it belongs
  and position within this snapshot. There are @c get_...() methods for getting
  objects given their coordinates. Objects can be also browsed using one of
  the iterator classes defined within @c Image_info.

  For each type of objects stored in the catalogue, there is a class derived
  from @c Image_info::Obj, whose instances are used to keep information about
  such objects. These instances are owned by the @c Image_info object who is 
  responsible for freeing memory used by them. Currently a memory root is used
  for allocating this memory.
 */
class Image_info: public st_bstream_image_header
{
public: // public interface

   // datatypes
   
   typedef enum_bstream_item_type obj_type;

   class Obj;   ///< Base for all object classes.
   class Db;    ///< Class representing a database.
   class Table; ///< Class representing a table.

   class Iterator;      ///< Base for all iterators.
   class Db_iterator;   ///< Iterates over all databases.
   class DbObj_iterator;  ///< Iterates over tables in a database.

   virtual ~Image_info();
 
   // info about image (most of it is in the st_bstream_image_header base

   size_t     data_size;      ///< How much of table data is saved in the image.

   uint       table_count() const;
   uint       db_count() const;
   uint       snap_count() const;

   // Retrieve objects using their coordinates.

   Db*    get_db(uint pos) const;
   Table* get_table(uint snap_no, ulong pos) const;

   // Iterators for enumerating the contents of the archive.

   Db_iterator*    get_dbs();
   DbObj_iterator* get_db_objects(const Db &db);

   /**
     Pointers to Snapshot_info objects corresponding to snapshots
     present in the image. 
    */ 
   Snapshot_info *m_snap[256];
   
   // save timing & binlog info 
   
   void save_start_time(const time_t time);   
   void save_end_time(const time_t time);
   void save_vp_time(const time_t time);   

   void save_binlog_pos(const ::LOG_INFO&);

 protected: // internal interface
  
  // Populate the catalogue
  
  int    add_snapshot(Snapshot_info&);
  Db*    add_db(const String &db_name, ulong pos);
  Table* add_table(Db &db, const String &table_name, 
                   Snapshot_info &snap, ulong pos);

 // IMPLEMENTATION

 protected:

  Image_info();
  uint m_table_count;

 private:

  class Tables; ///< Implementation of Table_list interface. 

  // storage

  MEM_ROOT  mem_root;    ///< Memory root used to allocate @c Obj instaces.
  Map<uint,Db>   m_dbs;  ///< Pointers to Db instances.

  // friends

  friend class Snapshot_info;
};

/**
  Find object in a catalogue.
  
  The object is identified by its coordinates stored in a 
  @c st_bstream_item_info structure.
  
  @returns Pointer to the corresponding @c Obj instance or NULL if object
  was not found.
 */ 
Image_info::Obj* find_obj(const Image_info &info, 
                          const st_bstream_item_info &item);


/**
  Implements Table_list interface.
  
  When list of tables is passed to a backup/restore driver, it is seen
  by the driver as an object of abstract type Table_list. This class implements
  that interface using a map, which for given table number returns a pointer
  to corresponding @c Image_info::Table instance.
  
  This class is not a container - it only stores pointers to 
  @c Image_info::Table objects which are owned by the @c Image_info instance.
 */ 
class Image_info::Tables:
  public Table_list,
  public Map<uint,Image_info::Table>
{
  typedef Map<uint,Image_info::Table> Base;
 
 public:

  Tables(ulong, ulong);
  ulong count() const;
  Table_ref operator[](ulong) const;
  Image_info::Table* get_table(ulong) const;
};

/**
  Create instance of @c Image_info::Tables class.
  
  The parameters determine how memory is allocated.
  
  @param[in] init_size  the initial number of slots
  @param[in] increase   the amount by which allocated memory is increased
                        when the current capacity is exceeded
 */ 
inline
Image_info::Tables::Tables(ulong init_size, ulong increase): 
  Base(init_size, increase) 
{}


/**
  Describes table data snapshot stored inside backup image.

  Such snapshot is created by a backup driver and read by a restore driver. For
  each type of snapshot a separate class is derived from @c Snapshot_info.
  Currently we support 
  
  - native snapshots (created by native backup engines),
  - CS snapshot      (created by built-in backup engine using consistent read),
  - default snapshot (created by built-in default backup engine).
  
  A @c Snapshot_info instance stores the list of tables whose data are stored
  in the snapshot. It also has methods for determining whether a given table 
  can be added to the snapshot or not, and for creating backup/restore drivers 
  for processing the snapshot.
 */
class Snapshot_info
{
 public:

  enum enum_snap_type {
    NATIVE_SNAPSHOT= BI_NATIVE,   ///< snapshot created by native backup engine
    DEFAULT_SNAPSHOT= BI_DEFAULT, ///< snapshot created by built-in, blocking backup engine
    CS_SNAPSHOT= BI_CS            ///< snapshot created by built-in CS backup engine
  };

  virtual enum_snap_type type() const =0; 
  version_t version() const; ///< Returns version of snapshot's format.
  
  /**
    Position inside image's snapshot list.

    Starts with 1. M_no == 0 means that this snapshot is not included in the
    backup image (for example, no tables have been added to it yet).
  */
  ushort m_no;

  /**
    Size of the initial data transfer (estimate). This is
    meaningful only after a call to get_backup_driver().
  */
  size_t init_size;

  /**
    Return name identifying the snapshot in debug messages.

    The name should fit into "%s backup/restore driver" pattern.
   */
  virtual const char* name() const;

  /// Check if instance was correctly constructed
  virtual bool is_valid() =0;

  ulong table_count() const;
  
  /// Determine if a table using given storage engine can be saved in this image.
  virtual bool accept(const Table_ref&, const storage_engine_ref) =0;

  /// Create backup driver for the image.
  virtual result_t get_backup_driver(Backup_driver*&) =0;

  /// Create restore driver for the image.
  virtual result_t get_restore_driver(Restore_driver*&) =0;

  virtual ~Snapshot_info();

 protected:
 
  version_t m_version; ///< to store version of the snapshot's format

  Snapshot_info(const version_t);

  // Methods for adding and accessing tables stored in the table list.

  int add_table(Image_info::Table &t, ulong pos);
  Image_info::Table* get_table(ulong pos);
 
  // IMPLEMENTATION
 
  Image_info::Tables m_tables; ///< List of tables stored in this image.

  friend class Image_info;
};


inline
Snapshot_info::Snapshot_info(const version_t version): 
  m_no(0), init_size(0), m_version(version), m_tables(128,1024)
{}

inline
Snapshot_info::~Snapshot_info()
{}

/********************************************************************
 
   Classes for representing various object types.
 
 ********************************************************************/ 

/**
  Represents object stored in a backup image.

  Instances of this class store name of the object and other relevant
  information. For each type of objects a subclass of this class is derived
  which is specialized in storing information about that kind of objects.

  Method @c info() returns a pointer to @c st_bstream_item_info structure 
  filled with data describing the corresponding object in the way required by
  backup stream library.
  
  Method @c materialize() can be used to create a corresponding instance of
  @c obs::Obj, to be used by server's objects services API. If @c m_obj_ptr is
  not NULL then it contains a pointer to the corresponding @c obs:;Obj instance
  which was obtained earlier (wither with @c materialize() or from server's 
  object enumerators).
*/
class Image_info::Obj: public Sql_alloc
{
 public:
 
  /* 
    Note: Snice we are using Sql_alloc and allocate instances using MEM_ROOT,
    destructors will not be called! This is also true for derived classes.
   */
  virtual ~Obj();

  /**
    Returns pointer to @c st_bstream_item_info structure filled with data about
    the object.
   */ 
  virtual const st_bstream_item_info* info() const =0;

  /**
    Pointer to the corresponding @c obs::Obj instance, if it is known.
   */ 
  obs::Obj  *m_obj_ptr;
  
  /**
    Create corresponding @c obs::Obj instance from a serialization string.
   */ 
  virtual obs::Obj *materialize(uint ver, const String&) =0;

 protected:

  String m_name;  ///< For storing object's name.

  void store_name(const String&); 

  Obj();

  friend class Image_info;
};

inline
Image_info::Obj::Obj(): m_obj_ptr(NULL)
{}

inline
Image_info::Obj::~Obj()
{}

/**
  Specialization of @c Image_info::Obj for storing info about a database.
*/
class Image_info::Db
 : public st_bstream_db_info,
   public Image_info::Obj,
   public Db_ref
{
  ulong m_table_count;  ///< Number of tables belonging to that database.

 public:

  Db(const String&);

  const st_bstream_item_info* info() const;
  obs::Obj* materialize(uint ver, const String &sdata);
  result_t add_table(Table&);

 private:
 
  Table *first_table; ///< Pointer to the first table in this database's table list. 
  Table *last_table;  ///< Pointer to the last table in this database's table list.

  friend class DbObj_iterator;
};

inline
Image_info::Db::Db(const String &name):
 Db_ref(Image_info::Obj::m_name),
 m_table_count(0), first_table(NULL), last_table(NULL)
{
  bzero(&base,sizeof(base));
  base.type= BSTREAM_IT_DB;
  store_name(name);
}


/**
  Specialization of @c Image_info::Obj for storing info about a table.
*/
class Image_info::Table
 : public st_bstream_table_info,
   public Image_info::Obj,
   public Table_ref
{
  const Db &m_db;     ///< The database to which this table belongs.
  Table  *next_table; ///< Used to crate a linked list of tables in a database.

 public:

  Table(const Db &db, const String &name);

  const st_bstream_item_info* info() const;
  obs::Obj* materialize(uint ver, const String &sdata);

  friend class Db;
  friend class DbObj_iterator;
};

inline
Image_info::Table::Table(const Db &db, const String &name):
  Table_ref(db.name(), Image_info::Obj::m_name), m_db(db), next_table(NULL)
{
  bzero(&base,sizeof(base));
  base.base.type= BSTREAM_IT_TABLE;
  store_name(name);
}


/********************************************************************
 
   Iterators
 
 ********************************************************************/ 

/**
  Base class for all iterators.
  
  An iterator is used as follows
  @code
  Iterator_X      it;
  Image_info::Obj *obj;
  
  while ((obj= it++))
  {
    <do something with obj>
  }
  @endcode

  This is an abstract class. Derived iterators must define @c get_ptr() and
  @c next() methods which are used to implement @c operator++().
 */ 
class Image_info::Iterator
{
 public:

  Iterator(const Image_info &info);
  virtual ~Iterator();

  const Obj* operator++(int);

 protected:

  const Image_info &m_info;

 private:

  /** 
    Return pointer to the current object of the iterator.
   
    Returns NULL if iterator is past the last object in the sequence.
   */
  virtual const Obj* get_ptr() const =0;
  
  /** 
    Move iterator to next object.
   
    Returns FALSE if there are no more objects to enumerate.
   */
  virtual bool next() =0;
};

inline
Image_info::Iterator::Iterator(const Image_info &info): 
 m_info(info) 
{}

inline
Image_info::Iterator::~Iterator() 
{}

/**
  Used to iterate over all databases stored in a backup image.
 */ 
class Image_info::Db_iterator
 : public Image_info::Iterator
{
 public:

  Db_iterator(const Image_info&);

 protected:

  uint pos;
  const Obj* get_ptr() const;
  bool next();
};

inline
Image_info::Db_iterator::Db_iterator(const Image_info &info): 
  Iterator(info), pos(0)
{}

/**
  Used to iterate over all objects belonging to a given database.
  
  Currently only tables are enumerated.
 */ 
class Image_info::DbObj_iterator
 : public Image_info::Db_iterator
{
  const Table *ptr;

 public:

  DbObj_iterator(const Image_info&, const Db&);

 private:

  const Obj* get_ptr() const;
  bool next();
};

inline
Image_info::DbObj_iterator::DbObj_iterator(const Image_info &info, const Db &db):
 Db_iterator(info), ptr(db.first_table)
{}


/********************************************************************
 
   Inline members of Image_info class 
 
 ********************************************************************/ 

/// Returns number of databases in the image.
inline
uint Image_info::db_count() const
{ 
  return m_dbs.count();
}

/// Returns total number of tables in the image.
inline
uint Image_info::table_count() const
{ 
  return m_table_count;
}

/// Returns number of snapshots used by the image.
inline
uint Image_info::snap_count() const
{ 
  return st_bstream_image_header::snap_count;
}

/**
  Save time inside a @c bstream_time_t structure (helper function).
 */ 
inline
static
void save_time(const time_t t, bstream_time_t &buf)
{
  struct tm time;
  gmtime_r(&t,&time);
  buf.year= time.tm_year;
  buf.mon= time.tm_mon;
  buf.mday= time.tm_mday;
  buf.hour= time.tm_hour;
  buf.min= time.tm_min;
  buf.sec= time.tm_sec;  
}

/**
  Store backup/restore start time inside image's header.
 */ 
inline
void Image_info::save_start_time(const time_t time)
{
  save_time(time, start_time);
}

/**
  Store backup/restore end time inside image's header.
 */ 
inline
void Image_info::save_end_time(const time_t time)
{
  save_time(time, end_time);
}

/**
  Store validity point time inside image's header.
 */ 
inline
void Image_info::save_vp_time(const time_t time)
{
  save_time(time, vp_time);
}

/**
  Store validity point binlog position inside image's header.
 */ 
inline
void Image_info::save_binlog_pos(const ::LOG_INFO &li)
{
  binlog_pos.pos= (unsigned long int)li.pos;
  binlog_pos.file= const_cast<char*>(li.log_file_name);
}

/********************************************************************
 
   Inline members of Image_info::Tables class.
 
 ********************************************************************/ 

/// Return number of tables in the list.
inline
ulong Image_info::Tables::count() const
{ return Base::count(); }

/** 
  Return table stored at a given position.
 
  @returns pointer to the @c Image_info::Table instance stored at
  position @c pos or NULL if that position is empty.
 */
inline
Image_info::Table* Image_info::Tables::get_table(ulong pos) const
{ 
  return Base::operator[](pos);
} 

/// Implementation of @c Table_list virtual method.
inline
Table_ref Image_info::Tables::operator[](ulong pos) const
{ 
  Table *t= get_table(pos);
  DBUG_ASSERT(t);
  return *t; 
}

/********************************************************************
 
   Inline members of Snapshot_info::Obj and derived classes.
 
 ********************************************************************/ 

/**
  Store objects name inside the object.
  
  The name is also stored inside the corresponding @c st_bstream_item_info
  structure (just pointer).
 */ 
inline
void Image_info::Obj::store_name(const String &name)
{
  m_name.copy(name);
  st_bstream_item_info *info= const_cast<st_bstream_item_info*>(this->info());
  info->name.begin= (byte*)name.ptr();
  info->name.end= info->name.begin + name.length();
}


/// Implementation of @c Image_info::Obj virtual method.
inline
const st_bstream_item_info* Image_info::Db::info() const 
{
  return &base; 
}

/// Implementation of @c Image_info::Obj virtual method.
inline
const st_bstream_item_info* Image_info::Table::info() const 
{
  return &base.base; 
}

/// Implementation of @c Image_info::Obj virtual method.
inline
obs::Obj* Image_info::Db::materialize(uint ver, const String &sdata)
{
  return m_obj_ptr= obs::materialize_database(&name(), ver, &sdata); 
}

/// Implementation of @c Image_info::Obj virtual method.
inline
obs::Obj* Image_info::Table::materialize(uint ver, const String &sdata)
{
  return m_obj_ptr= obs::materialize_table(&db().name(), &name(), ver, &sdata);
}

/**
  Add table to a database.
  
  The table is appended to database's table list.
 */
inline
result_t Image_info::Db::add_table(Table &tbl)
{
  tbl.next_table= NULL;
  
  if (!last_table)
    first_table= last_table= &tbl;
  else
  {
    last_table->next_table= &tbl;
    last_table= &tbl;
  }
  
  m_table_count++;
    
  return OK;
}


/********************************************************************
 
   Inline members of Snapshot_info class.
 
 ********************************************************************/ 

/// version of snapshot's format
inline
version_t Snapshot_info::version() const  
{ return m_version; }

/// Default implementation of the virtual method
inline 
const char* Snapshot_info::name() const
{ return "<Unknown>"; }

/// Add table at a given position.
inline
int Snapshot_info::add_table(Image_info::Table &t, ulong pos)
{
  return m_tables.insert(pos, &t);
}

/// Get table at a given position
inline
Image_info::Table* Snapshot_info::get_table(ulong pos)
{
  return m_tables.get_table(pos);
}

/// Return number of tables stored in this snapshot.
inline
ulong Snapshot_info::table_count() const
{
  return m_tables.count();
}

/********************************************************************
 
   Inline members of iterators.
 
 ********************************************************************/ 

inline
const Image_info::Obj* Image_info::Iterator::operator++(int)
{
  const Obj *obj= get_ptr();
  next();
  return obj; 
}

/// Implementation of @c Image_info::Iterator virtual method.
inline
const Image_info::Obj* Image_info::Db_iterator::get_ptr() const
{
  /*
    There should be no "holes" in the sequence of databases. That is,
    if there are N databases in the catalogue then for i=0,1,..,N-1, 
    m_info.m_dbs[i] should store pointer to the i-th database.
   */ 
  DBUG_ASSERT(pos >= m_info.db_count() || m_info.m_dbs[pos]);
  return m_info.m_dbs[pos];
}

/// Implementation of @c Image_info::Iterator virtual method.
inline
bool Image_info::Db_iterator::next()
{
  if (pos < m_info.db_count())
  {
    pos++;
    return TRUE;
  }
  else
    return FALSE;
}

/// Implementation of @c Image_info::Iterator virtual method.
inline
const Image_info::Obj* Image_info::DbObj_iterator::get_ptr() const
{
  return ptr;
}

/// Implementation of @c Image_info::Iterator virtual method.
inline
bool Image_info::DbObj_iterator::next()
{
  if (ptr)
    ptr= ptr->next_table;
  return ptr != NULL;
}

} // backup namespace

namespace backup {

/*
 Wrappers around backup stream functions which perform necessary type conversions.

 TODO: report errors
*/

inline
result_t
write_preamble(const Image_info &info, OStream &s)
{
  const st_bstream_image_header *hdr= static_cast<const st_bstream_image_header*>(&info);
  int ret= bstream_wr_preamble(&s, const_cast<st_bstream_image_header*>(hdr));
  return ret == BSTREAM_ERROR ? ERROR : OK;
}

inline
result_t
write_summary(const Image_info &info, OStream &s)
{
  const st_bstream_image_header *hdr= static_cast<const st_bstream_image_header*>(&info);
  int ret= bstream_wr_summary(&s, const_cast<st_bstream_image_header*>(hdr));
  return ret == BSTREAM_ERROR ? ERROR : OK;
}

inline
result_t
read_header(Image_info &info, IStream &s)
{
  int ret= bstream_rd_header(&s, static_cast<st_bstream_image_header*>(&info));
  return ret == BSTREAM_ERROR ? ERROR : OK;
}

inline
result_t
read_catalog(Image_info &info, IStream &s)
{
  int ret= bstream_rd_catalogue(&s, static_cast<st_bstream_image_header*>(&info));
  return ret == BSTREAM_ERROR ? ERROR : OK;
}

inline
result_t
read_meta_data(Image_info &info, IStream &s)
{
  int ret= bstream_rd_meta_data(&s, static_cast<st_bstream_image_header*>(&info));
  return ret == BSTREAM_ERROR ? ERROR : OK;
}

inline
result_t
read_summary(Image_info &info, IStream &s)
{
  int ret= bstream_rd_summary(&s, static_cast<st_bstream_image_header*>(&info));
  return ret == BSTREAM_ERROR ? ERROR : OK;
}

} // backup namespace

#endif /*CATALOG_H_*/
