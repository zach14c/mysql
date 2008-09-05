#ifndef CATALOG_H_
#define CATALOG_H_

/**
  @file
  
  @todo Fix error logging in places marked with "FIXME: error logging...". In 
  these places it should be decided if and how the error should be shown to the
  user. If an error message should be logged, it can happen either in the place
  where error was detected or somewhere up the call stack.
*/ 

#include <si_objects.h>
#include <backup_stream.h> // for st_bstream_* types
#include <backup/backup_aux.h>  // for Map template

class Backup_restore_ctx;

namespace backup {

/********************************************************************
 
   Image_info and Snapshot_info classes.
 
 ********************************************************************/ 

class Snapshot_info;
class Logical_snapshot;

/**
  Describes contents of a backup image.

  This class stores a catalogue of a backup image, that is, description of
  all objects stored in it (currently only databases and tables).

  Only object names are stored in the catalogue. Other object data is stored
  in the meta-data part of the image and in case of tables, their data is
  stored in table data snapshots created by backup drivers.

  For each snapshot present in the image there is a @c Snapshot_info object.
  A pointer to n-th snapshot object is stored in @c m_snap[n]. This object 
  contains list of tables whose data is stored in the snapshot. Note that each 
  table in the catalogue must belong to exactly one snapshot.

  Each object in the catalogue has its coordinates. The format of these 
  coordinates depends on the object type. For databases, it is just its number. 
  For tables, its coordinates are the number of the snapshot to which it belongs
  and position within this snapshot. There are @c get_...() methods for getting
  objects given their coordinates. Objects can be also browsed using one of
  the iterator classes defined within @c Image_info.

  For each type of object stored in the catalogue, there is a class derived
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
   class Ts;    ///< Class representing a tablespace.
   class Db;    ///< Class representing a database.
   class Table; ///< Class representing a table.
   class Dbobj; ///< Class representing a per-database object other than table.

   class Iterator;      ///< Base for all iterators.
   class Ts_iterator;   ///< Iterates over all tablespaces.
   class Db_iterator;   ///< Iterates over all databases.
   class Dbobj_iterator;  ///< Iterates over objects in a database.

   virtual ~Image_info();
 
   // info about image (most of it is in the st_bstream_image_header base

   size_t     data_size;      ///< How much of table data is saved in the image.

   ulong      table_count() const;
   uint       db_count() const;
   uint       ts_count() const;
   ushort     snap_count() const;

   // Examine contents of the catalogue.

   bool has_db(const String&);

   // Retrieve objects using their coordinates.

   Db*    get_db(uint pos) const;
   Ts*    get_ts(uint pos) const;
   Dbobj* get_db_object(uint db_num, ulong pos) const;
   Table* get_table(ushort snap_num, ulong pos) const;

   // Iterators for enumerating the contents of the archive.

   Db_iterator*     get_dbs() const;
   Ts_iterator*     get_tablespaces() const;
   Dbobj_iterator*  get_db_objects(const Db &db) const;

   /**
     Pointers to @c Snapshot_info objects corresponding to the snapshots
     present in the image.
     
     We can have at most 256 different snapshots which is a limitation imposed
     by the backup stream library (the number of snapshots is stored inside 
     backup image using one byte field).
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
  Db*    add_db(const String &db_name, uint pos);
  Ts*    add_ts(const String &db_name, uint pos);
  Dbobj* add_db_object(Db &db, const obj_type type,
                       const ::String &name, ulong pos);
  Table* add_table(Db &db, const ::String &table_name, 
                   Snapshot_info &snap, ulong pos);

 // IMPLEMENTATION

 protected:

  Image_info();
  uint m_table_count;
  MEM_ROOT  mem_root;    ///< Memory root for storage of catalogue items.

  class Tables; ///< Implementation of Table_list interface. 

 private:

  Map<uint, Db>   m_dbs; ///< Pointers to Db instances.
  Map<uint, Ts>   m_ts_map; ///< Pointers to Ts instances.
  String    m_binlog_file; ///< To store binlog file name at VP time.

  // friends

  friend class Snapshot_info;
  friend class backup::Logical_snapshot; // needs access to Tables class
};

Image_info::Obj* find_obj(const Image_info &info, 
                          const st_bstream_item_info &item);


/**
  Implements Table_list interface.
  
  When list of tables is passed to a backup/restore driver, it is seen
  by the driver as an object of abstract type Table_list. This class implements
  that interface using a map, which for given table number returns a pointer
  to corresponding @c Image_info::Table instance.
  
  @note This class is not a container - it only stores pointers to 
  @c Image_info::Table objects which are owned by the @c Image_info instance.
 */ 
class Image_info::Tables:
  public Table_list,
  public Map<uint, Image_info::Table>
{
  typedef Map<uint, Image_info::Table> Base;
 
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
Image_info::Tables::Tables(ulong init_size, ulong increase)
  :Base(init_size, increase) 
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
    /** snapshot created by native backup engine. */
    NATIVE_SNAPSHOT= BI_NATIVE,
    /** Snapshot created by built-in, blocking backup engine. */
    DEFAULT_SNAPSHOT= BI_DEFAULT,
    /** Snapshot created by built-in CS backup engine. */
    CS_SNAPSHOT= BI_CS,
    /** snapshot created by No data backup driver. */
    NODATA_SNAPSHOT= BI_NODATA
  };

  virtual enum_snap_type type() const =0; 
  version_t version() const; ///< Returns version of snapshot's format.
  
  /**
    Position inside image's snapshot list.

    Starts with 1. @c M_num == 0 means that this snapshot is not included in the
    backup image (for example, no tables have been added to it yet).
  */
  ushort m_num;

  /**
    Size of the initial data transfer (estimate). This is
    meaningful only after a call to get_backup_driver().
  */
  size_t init_size;

  /**
    Return name identifying the snapshot in debug messages.

    The name should fit into "%s backup/restore driver" pattern.
   */
  virtual const char* name() const =0;
               
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

  Image_info::Table* get_table(ulong pos) const;

 protected:
 
  version_t m_version; ///< Stores version number of the snapshot's format.

  Snapshot_info(const version_t);

  // Methods for adding and accessing tables stored in the table list.

  int add_table(Image_info::Table &t, ulong pos);

  // IMPLEMENTATION
 
  Image_info::Tables m_tables; ///< List of tables stored in this image.

  friend class Image_info;
};


inline
Snapshot_info::Snapshot_info(const version_t version) 
  :m_num(0), init_size(0), m_version(version), m_tables(128, 1024)
{}

inline
Snapshot_info::~Snapshot_info()
{}

/********************************************************************
 
   Classes for representing various object types.
 
 ********************************************************************/ 

/**
  Represents object stored in a backup image.

  Instances of this class store the name and other relevant information about
  an object. For each type of object a subclass of this class is derived
  which is specialized in storing information specific to that kind of object.

  Method @c info() returns a pointer to @c st_bstream_item_info structure 
  filled with data describing the corresponding object in the way required by
  backup stream library.
  
  Method @c materialize() can be used to create a corresponding instance of
  @c obs::Obj, to be used by server's objects services API. If @c m_obj_ptr is
  not NULL then it contains a pointer to the corresponding @c obs::Obj instance
  which was obtained earlier (either with @c materialize() or from server's 
  object iterators). The @c Obj instance owns the server object and is 
  responsible for deleting it.
*/
class Image_info::Obj: public Sql_alloc
{
 public:
 
  /* 
    Note: Since we are using Sql_alloc and allocate instances using MEM_ROOT,
    destructors will not be called! This is also true for derived classes.
   */
  virtual ~Obj();

  obj_type type() const;

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
  virtual obs::Obj *materialize(uint ver, const ::String&) =0;

  typedef Table_ref::name_buf describe_buf;
  virtual const char* describe(describe_buf&) const =0;

 protected:

  String m_name;  ///< For storing object's name.

  void store_name(const String&); 

  Obj();

  friend class Image_info;
};

inline
Image_info::Obj::Obj() :m_obj_ptr(NULL)
{}

inline
Image_info::Obj::~Obj()
{
  // Delete corresponding server object if present.
  delete m_obj_ptr;
}


/**
  Specialization of @c Image_info::Obj for storing info about a tablespace.
*/
class Image_info::Ts
 : public st_bstream_ts_info,
   public Image_info::Obj
{
 public:

  Ts(const ::String&);

  const st_bstream_item_info* info() const;
  const st_bstream_ts_info* ts_info() const;
  obs::Obj* materialize(uint ver, const ::String &sdata);
  const char* describe(describe_buf&) const;
};

inline
Image_info::Ts::Ts(const ::String &name)
{
  bzero(&base, sizeof(base));
  base.type= BSTREAM_IT_TABLESPACE;
  store_name(name);
}


/**
  Specialization of @c Image_info::Obj for storing info about a database.
*/
class Image_info::Db
 : public st_bstream_db_info,
   public Image_info::Obj,
   public Db_ref
{
  ulong m_obj_count;    ///< Number of non-table objects in the database.

 public:

  Db(const ::String&);

  const st_bstream_item_info* info() const;
  const st_bstream_db_info* db_info() const;
  ulong obj_count() const;
  obs::Obj* materialize(uint ver, const ::String &sdata);
  result_t add_obj(Dbobj&, ulong pos);
  Dbobj*   get_obj(ulong pos) const;
  result_t add_table(Table&);
  const char* describe(describe_buf&) const;

 private:
 
  Table *first_table; ///< Pointer to the first table in database's table list. 
  Table *last_table;  ///< Pointer to the last table in database's table list.

  /**
    For n-th object in this databse, @c m_objs[n] is a pointer to the
    corresponding Dbobj instance.
   */ 
  Map<ulong, Dbobj> m_objs;

  friend class Dbobj_iterator;
  friend class Perdb_iterator;
};

inline
Image_info::Db::Db(const ::String &name)
 :Db_ref(Image_info::Obj::m_name),
  m_obj_count(0), first_table(NULL), last_table(NULL), m_objs(128)
{
  bzero(&base, sizeof(base));
  base.type= BSTREAM_IT_DB;
  store_name(name);
}


/**
  Specialization of @c Image_info::Obj for storing info about a per-database
  object.

  @note For tables, there is dedicated class @c Image_info::Table.
*/
class Image_info::Dbobj
  : public st_bstream_dbitem_info,
    public Image_info::Obj,
    public Table_ref
{
  const Db &m_db;     ///< The database to which this obj belongs.

 public:

  Dbobj(const Db &db, const obj_type type, const ::String &name);

  const st_bstream_item_info* info() const;
  obs::Obj* materialize(uint ver, const ::String &sdata);
  const char* describe(Obj::describe_buf&) const;

  friend class Db;
  friend class Dbobj_iterator;
};

inline
Image_info::Dbobj::Dbobj(const Db &db, const obj_type type,
                         const ::String &name)
  :Table_ref(db.name(), Image_info::Obj::m_name), m_db(db)
{
  bzero(&base, sizeof(base));
  base.type= type;
  st_bstream_dbitem_info::db= const_cast<st_bstream_db_info*>(m_db.db_info());
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
  TABLE_LIST  *m_table; ///< If not NULL, points at opened table.

 public:

  Table(const Db &db, const ::String &name);

  const st_bstream_item_info* info() const;
  obs::Obj* materialize(uint ver, const ::String &sdata);
  const char* describe(Obj::describe_buf&) const;

  friend class Db;
  friend class Dbobj_iterator;
  friend class Logical_snapshot;     // reads m_table
  friend class ::Backup_restore_ctx; // sets m_table
};

inline
Image_info::Table::Table(const Db &db, const ::String &name)
  :Table_ref(db.name(), Image_info::Obj::m_name), m_db(db), next_table(NULL),
   m_table(NULL)
{
  bzero(&base, sizeof(base));
  base.base.type= BSTREAM_IT_TABLE;
  base.db= const_cast<st_bstream_db_info*>(db.db_info());
  snap_num= 0;
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

  Obj* operator++(int);

 protected:

  const Image_info &m_info;

 private:

  /** 
    Return pointer to the current object of the iterator.
   
    Returns NULL if iterator is past the last object in the sequence.
   */
  virtual Obj* get_ptr() const =0;
  
  /** 
    Move iterator to next object.
   
    Returns FALSE if there are no more objects to enumerate.
   */
  virtual bool next() =0;
};

inline
Image_info::Iterator::Iterator(const Image_info &info) :m_info(info) 
{}

inline
Image_info::Iterator::~Iterator() 
{}


/**
  Used to iterate over all tablespaces stored in a backup image.

  @note Backup stream library infers position of each tablespace in the catalogue
  from the order in which they are enumerated by this iterator. Therefore it
  is important that tablespaces are listed in correct order - first tablespace 
  at position 0, then at position 1 and so on.
 */ 
class Image_info::Ts_iterator
 : public Image_info::Iterator
{
 public:

  Ts_iterator(const Image_info&);

 protected:

  uint pos;
  Obj* get_ptr() const;
  bool next();
};

inline
Image_info::Ts_iterator::Ts_iterator(const Image_info &info)
  :Iterator(info), pos(0)
{}


/**
  Used to iterate over all databases stored in a backup image.

  @note Backup stram library infers position of each database in the catalogue
  from the order in which they are enumerated by this iterator. Therefore it
  is important that databases are listed in correct order - first database at
  position 0, then at position 1 and so on.
 */ 
class Image_info::Db_iterator
 : public Image_info::Iterator
{
 public:

  Db_iterator(const Image_info&);

 protected:

  uint pos;
  Obj* get_ptr() const;
  bool next();
};

inline
Image_info::Db_iterator::Db_iterator(const Image_info &info)
  :Iterator(info), pos(0)
{}


/**
  Used to iterate over all objects belonging to a given database.

  @note Backup stream library infers position of each non-table object within
  database's catalogue from the order in which this iterator enumenrates them.
  Therefore it is important that objects are listed in correct order - first
  all tables should be listed, then the non-table object stored at position 0,
  then at position 1 and so on.
 */
class Image_info::Dbobj_iterator
 : public Image_info::Db_iterator
{
  const Db    &m_db;
  Table *ptr;
  ulong pos;

 public:

  Dbobj_iterator(const Image_info&, const Db&);

 private:

  Obj* get_ptr() const;
  bool next();
};

inline
Image_info::Dbobj_iterator::Dbobj_iterator(const Image_info &info, const Db &db)
 :Db_iterator(info), m_db(db), ptr(db.first_table), pos(0)
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

/// Returns number of tablespaces in the image.
inline
uint Image_info::ts_count() const
{ 
  return m_ts_map.count();
}

/// Returns total number of tables in the image.
inline
ulong Image_info::table_count() const
{ 
  return m_table_count;
}

/// Returns number of snapshots used by the image.
inline
ushort Image_info::snap_count() const
{ 
  return st_bstream_image_header::snap_count;
}


/**
  Return database stored in the catalogue.

  @param[in]  pos positon of the database in the catalogue

  @returns Pointer to @c Image_info::Db instance storing information 
  about the database or NULL if no database is stored at given position.
 */ 
inline
Image_info::Db* Image_info::get_db(uint pos) const
{
  return m_dbs[pos];
}

/**
  Return tablespace stored in the catalogue.

  @param[in]  pos positon of the tablespace in the catalogue

  @returns Pointer to @c Image_info::Ts instance storing information 
  about the tablespace or NULL if no tablespace is stored at given position.
 */ 
inline
Image_info::Ts* Image_info::get_ts(uint pos) const
{
  return m_ts_map[pos];
}


/**
  Save time inside a @c bstream_time_t structure (helper function).
 */ 
inline
static
void save_time(const time_t t, bstream_time_t &buf)
{
  struct tm time;
  gmtime_r(&t, &time);
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
  Store validity point binlog position inside image's header. Also sets
  BSTREAM_FLAG_BINLOG in @c flags bitmap to indicate that this
  backup image contains a valid binlog position.
 */ 
inline
void Image_info::save_binlog_pos(const ::LOG_INFO &li)
{
  // save current binlog file name
  m_binlog_file.length(0);
  m_binlog_file.append(li.log_file_name);

  // store binlog coordinates
  binlog_pos.pos= (unsigned long int)li.pos;
  binlog_pos.file= const_cast<char*>(m_binlog_file.ptr());

  // make flags bitmap reflect that this backup image contains a valid
  // binlog position
  flags|= BSTREAM_FLAG_BINLOG;
}

/// Returns an iterator enumerating all databases stored in backup catalogue.
inline
Image_info::Db_iterator* Image_info::get_dbs() const
{
  // FIXME: error logging (in case allocation fails).
  return new Db_iterator(*this);
}

/// Returns an iterator enumerating all tablespaces stored in backup catalogue.
inline
Image_info::Ts_iterator* Image_info::get_tablespaces() const
{
  // FIXME: error logging (in case allocation fails).
  return new Ts_iterator(*this);
}

/// Returns an iterator enumerating all objects in a given database.
inline
Image_info::Dbobj_iterator* Image_info::get_db_objects(const Db &db) const
{
  // FIXME: error logging (in case allocation fails).
  return new Dbobj_iterator(*this, db);
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
 
   Inline members of Image_info::Obj and derived classes.
 
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

inline
Image_info::obj_type  Image_info::Obj::type() const
{
  return info()->type;
}


/// Implementation of @c Image_info::Obj virtual method.
inline
const st_bstream_item_info* Image_info::Db::info() const 
{
  return &base; 
}

inline
const st_bstream_db_info* Image_info::Db::db_info() const 
{
  return this; 
}

/// Implementation of @c Image_info::Obj virtual method.
inline
const st_bstream_item_info* Image_info::Ts::info() const 
{
  return &base; 
}

inline
const st_bstream_ts_info* Image_info::Ts::ts_info() const 
{
  return this; 
}


/// Implementation of @c Image_info::Obj virtual method.
inline
const st_bstream_item_info* Image_info::Table::info() const 
{
  return &base.base; 
}

/// Implementation of @c Image_info::Obj virtual method.
inline
const st_bstream_item_info* Image_info::Dbobj::info() const 
{
  return &base; 
}


/// Implementation of @c Image_info::Obj virtual method.
inline
const char* Image_info::Ts::describe(describe_buf &buf) const
{
  my_snprintf(buf, sizeof(buf), "`%s`", Obj::m_name.ptr());
  return buf;
}

/// Implementation of @c Image_info::Obj virtual method.
inline
const char* Image_info::Db::describe(describe_buf &buf) const
{
  my_snprintf(buf, sizeof(buf), "`%s`", Obj::m_name.ptr());
  return buf;
}

/// Implementation of @c Image_info::Obj virtual method.
inline
const char* Image_info::Table::describe(Obj::describe_buf &buf) const
{
  return Table_ref::describe(buf);
}

/// Implementation of @c Image_info::Obj virtual method.
inline
const char* Image_info::Dbobj::describe(Obj::describe_buf &buf) const
{
  return Table_ref::describe(buf);
}


/// Implementation of @c Image_info::Obj virtual method.
inline
obs::Obj* Image_info::Ts::materialize(uint ver, const ::String &sdata)
{
  delete m_obj_ptr;
  return m_obj_ptr= obs::materialize_tablespace(&m_name, ver, &sdata); 
}

/// Implementation of @c Image_info::Obj virtual method.
inline
obs::Obj* Image_info::Db::materialize(uint ver, const ::String &sdata)
{
  delete m_obj_ptr;
  return m_obj_ptr= obs::materialize_database(&name(), ver, &sdata); 
}

/// Implementation of @c Image_info::Obj virtual method.
inline
obs::Obj* Image_info::Table::materialize(uint ver, const ::String &sdata)
{
  delete m_obj_ptr;
  return m_obj_ptr= obs::materialize_table(&db().name(), &name(), ver, &sdata);
}

inline
obs::Obj* Image_info::Dbobj::materialize(uint ver, const ::String &sdata)
{ 
  const ::String *db_name= &Table_ref::db().name();
  const ::String *name= &Table_ref::name();

  delete m_obj_ptr;
  
  switch (base.type) {
  case BSTREAM_IT_VIEW:   
    m_obj_ptr= obs::materialize_view(db_name, name, ver, &sdata);
    break;
  case BSTREAM_IT_SPROC:  
    m_obj_ptr= obs::materialize_stored_procedure(db_name, name, ver, &sdata);
    break;
  case BSTREAM_IT_SFUNC:
    m_obj_ptr= obs::materialize_stored_function(db_name, name, ver, &sdata); 
    break;
  case BSTREAM_IT_EVENT:
    m_obj_ptr= obs::materialize_event(db_name, name, ver, &sdata);
    break;
  case BSTREAM_IT_TRIGGER:   
    m_obj_ptr= obs::materialize_trigger(db_name, name, ver, &sdata);
    break;
  case BSTREAM_IT_PRIVILEGE:
  {
    /*
      Here we undo the uniqueness suffix for grants.
    */
    String new_name;
    new_name.copy(*name);
    new_name.length(new_name.length() - UNIQUE_PRIV_KEY_LEN);
    m_obj_ptr= obs::materialize_db_grant(db_name, &new_name, ver, &sdata);
    break;
  }
  default: m_obj_ptr= NULL;
  }

  return m_obj_ptr;
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
  
  return OK;
}

/**
  Add object other than table to a database.
  
  The object is stored in database's object list at given position.
 */ 
inline
result_t Image_info::Db::add_obj(Dbobj &obj, ulong pos)
{
  if (m_objs.insert(pos, &obj))
    return ERROR;

  m_obj_count++;

  return OK;
}

/// Get database object stored at given position.
inline
Image_info::Dbobj* Image_info::Db::get_obj(ulong pos) const
{
  return m_objs[pos];
}

/// Return number of objects, other than tables, belonging to database.
inline
ulong Image_info::Db::obj_count() const
{
  return m_obj_count;
}


/********************************************************************
 
   Inline members of Snapshot_info class.
 
 ********************************************************************/ 

/// version of snapshot's format
inline
version_t Snapshot_info::version() const  
{ return m_version; }

/// Add table at a given position.
inline
int Snapshot_info::add_table(Image_info::Table &t, ulong pos)
{
  return m_tables.insert(pos, &t);
}

/// Get table at a given position
inline
Image_info::Table* Snapshot_info::get_table(ulong pos) const
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
Image_info::Obj* Image_info::Iterator::operator++(int)
{
  Obj *obj= get_ptr();
  next();
  return obj; 
}

/// Implementation of @c Image_info::Iterator virtual method.
inline
Image_info::Obj* Image_info::Db_iterator::get_ptr() const
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
Image_info::Obj* Image_info::Ts_iterator::get_ptr() const
{
  /*
    There should be no "holes" in the sequence of tablespaces. That is,
    if there are N tablespaces in the catalogue then for i=0,1,..,N-1, 
    m_info.m_ts_map[i] should store pointer to the i-th database.
   */ 
  DBUG_ASSERT(pos >= m_info.ts_count() || m_info.m_ts_map[pos]);
  return m_info.m_ts_map[pos];
}

/// Implementation of @c Image_info::Iterator virtual method.
inline
bool Image_info::Ts_iterator::next()
{
  if (pos < m_info.ts_count())
  {
    pos++;
    return TRUE;
  }
  else
    return FALSE;
}


/// Implementation of @c Image_info::Iterator virtual method.
inline
Image_info::Obj* Image_info::Dbobj_iterator::get_ptr() const
{
  return ptr ? static_cast<Obj*>(ptr) : m_db.get_obj(pos);
}

/// Implementation of @c Image_info::Iterator virtual method.
inline
bool Image_info::Dbobj_iterator::next()
{
  if (ptr)
    ptr= ptr->next_table;
  else
    pos++;

  return ptr != NULL || pos < m_db.obj_count();
}

} // backup namespace

#endif /*CATALOG_H_*/
