/**
  @file

  Implementation of @c Backup_info class. Method @c find_backup_engine()
  implements algorithm for selecting backup engine used to backup
  given table.
  
*/

#include "../mysql_priv.h"
#include "../ha_partition.h"

#include "backup_info.h"
#include "backup_kernel.h"
#include "be_native.h"
#include "be_default.h"
#include "be_snapshot.h"
#include "be_nodata.h"

/// Return storage engine of a given table.
static
storage_engine_ref get_storage_engine(THD *thd, const backup::Table_ref &tbl)
{
  storage_engine_ref se= NULL;
  char path[FN_REFLEN];

  const char *db= tbl.db().name().ptr();
  const char *name= tbl.name().ptr();

  ::build_table_filename(path, sizeof(path), db, name, "", 0);

  ::TABLE *table= ::open_temporary_table(thd, path, db, name,
                    FALSE /* don't link to thd->temporary_tables */,
                    OTM_OPEN);
  if (table)
  {
    se= plugin_ref_to_se_ref(table->s->db_plugin);

    /*
      Further check for underlying storage engine is needed
      if table is partitioned
    */

    storage_engine_ref se_tmp= NULL;

    if (table->part_info)
    {
      partition_info *p_info=  table->part_info;
      List_iterator<partition_element> p_it(p_info->partitions);
      partition_element *p_el;
      
      while ((p_el= p_it++))
      {
        if (!se_tmp)
        {
          se_tmp= hton2plugin[p_el->engine_type->slot];
          ::handlerton *h= se_hton(se_tmp);

          /* 
             Native drivers don't support partitioning. Let Falcon and
             InnoDB use Snapshot driver; all other storage engines use
             Default.
          */
          if (h->start_consistent_snapshot == NULL) 
            goto close; // This is not a Falcon or InnoDB storage engine

          continue;
        }

        // use Default driver if partitions have different storage engines
        if (se_tmp != hton2plugin[p_el->engine_type->slot])
          goto close;
      };
      
      se= se_tmp;
    }

 close:
  
    ::intern_close_table(table);
    my_free(table, MYF(0));
  }
  
  return se;
}

#ifndef DBUG_OFF

/**
  Dummy backup engine factory function.
  
  This factory function never creates any backup engines - it always returns
  ERROR. It is used in @c has_native_backup() method for testing purposes.
  
  @return Always ERROR. 
*/
static
Backup_result_t dummy_backup_engine_factory(handlerton*, Backup_engine* &eng)
{
  eng= NULL;
  return backup::ERROR;
}
#endif

static
bool has_native_backup(storage_engine_ref se)
{
  handlerton *hton= se_hton(se);

  return hton && hton->get_backup_engine;
}

/**
  Find backup engine which can backup data of a given table.

  @param[in] tbl  the table to be backed-up

  @returns pointer to a Snapshot_info instance representing 
  snapshot to which the given table can be added. 
 */
backup::Snapshot_info* 
Backup_info::find_backup_engine(const backup::Table_ref &tbl)
{
  using namespace backup;

  Table_ref::name_buf buf;
  Snapshot_info *snap= NULL;
  
  DBUG_ENTER("Backup_info::find_backup_engine");

  // See if table has native backup engine

  storage_engine_ref se= get_storage_engine(m_thd, tbl);
  
  if (!se)
  {
    m_log.report_error(ER_NO_STORAGE_ENGINE, tbl.describe(buf));
    DBUG_RETURN(NULL);
  }
  
  /*
    The code below is used to test the backup engine selection logic. 
    
    In case a storage engine defines the handlerton::get_backup_engine pointer 
    but the factory function pointed by it refuses to create a backup engine, 
    backup kernel should try to use built-in backup engines, the same as if 
    handlerton::get_backup_engine was NULL.
   
    To test this behaviour, if "backup_test_dummy_be_factory" debug symbol is
    defined (e.g., with --debug="d,backup_test_dummy_be_factory" option), we
    set the get_backup_engine pointer for MyISAM handlerton to point at the 
    dummy backup engine factory which never creates any engines.
  */ 

#ifndef DBUG_OFF
  backup_factory *saved_factory; // to save hton->get_backup_engine

  DBUG_EXECUTE_IF("backup_test_dummy_be_factory", 
    {
      handlerton *hton= se_hton(se);
      saved_factory= hton->get_backup_engine;
      if (hton == myisam_hton) 
        hton->get_backup_engine= dummy_backup_engine_factory;
    });
#endif
  
  snap= native_snapshots[se];
  
  if (!snap)
    if (has_native_backup(se))
    {
      Native_snapshot *nsnap= new Native_snapshot(m_log, se);

      /*
        Check if the snapshot object is valid - in particular has successfully
        created the native backup engine. If not, we will continue searching
        for a backup engine, trying the built-in ones.
      */
      if (nsnap && nsnap->is_valid())
      {
        snapshots.push_front(nsnap);
        native_snapshots.insert(se, nsnap);

        if (nsnap->accept(tbl, se))        
          snap= nsnap;
      }
      else
        delete nsnap;
    }
  
  /*
    If "backup_test_dummy_be_factory" is used, the hton->get_backup_engine
    pointer has been modified. Here we restore the original value.
   */ 
  DBUG_EXECUTE_IF("backup_test_dummy_be_factory", 
    {
      handlerton *hton= se_hton(se);
      hton->get_backup_engine= saved_factory;
    });

  /* 
    If we couldn't locate native snapshot for that table - iterate over
    all existing snapshots and see if one of them can accept the table.
    
    The order on the snapshots list determines the preferred backup method 
    for a table. The snapshots for the built-in backup engines are always 
    present at the end of this list so that they can be selected as a last
    resort.
  */
    
  if (!snap)
  {
    List_iterator<Snapshot_info> it(snapshots);
    
    while ((snap= it++))
      if (snap->accept(tbl, se))
        break;
  }

  if (!snap)
    m_log.report_error(ER_BACKUP_NO_BACKUP_DRIVER,tbl.describe(buf));
  
  DBUG_RETURN(snap);
}

/*************************************************

   Implementation of Backup_info class

 *************************************************/

/*
  Definition of Backup_info::Ts_hash_node structure used by Backup_info::ts_hash
  HASH.
 */ 

struct Backup_info::Ts_hash_node
{
  const String *name;	///< Name of the tablespace.
  Ts *it;               ///< Catalogue entry holding the tablespace (if exists).

  Ts_hash_node(const String*);

  static uchar* get_key(const uchar *record, size_t *key_length, my_bool);
  static void free(void *record);
};

inline
Backup_info::Ts_hash_node::Ts_hash_node(const String *name) :name(name), it(NULL)
{}

void Backup_info::Ts_hash_node::free(void *record)
{
  delete (Ts_hash_node*)record;
}

uchar* Backup_info::Ts_hash_node::get_key(const uchar *record, 
                                          size_t *key_length, 
                                          my_bool)
{
  Ts_hash_node *n= (Ts_hash_node*)record;

  // ts_hash entries are indexed by tablespace name.

  if (n->name && key_length)
    *key_length= n->name->length();

  return (uchar*)(n->name->ptr());
}

/**
  Represents a node in the dependency list.
  
  Such node can be an empty placeholder or store a pointer to a catalogue item
  in @c obj member. Dependency list nodes are kept in a hash and thus 
  @c Dep_node contains all required infrastructure: the @c key member to store
  a key string plus @c get_key() and @c free() functions used in @c HASH 
  operations.
 */ 
struct Backup_info::Dep_node: public Sql_alloc
{
  Dep_node *next;
  Dbobj *obj;
  String key;

  Dep_node(const ::String &db_name, const ::String &name, const obj_type type);
  Dep_node(const Dep_node&);

  static uchar* get_key(const uchar *record, size_t *key_length, my_bool);
  static void free(void *record);
};

/**
  Create an empty dependency list node for a given per-database object.

  The object is identified by its name, the name of the database to
  which it belongs, and its type.
 */ 
inline
Backup_info::Dep_node::Dep_node(const ::String &db_name, const ::String &name,
                                const obj_type type)
  :next(NULL), obj(NULL)
{
  key.length(0);
  // Add type to make sure keys are unique even between different namespaces
  key.set_int(type, TRUE, &my_charset_bin);
  key.append("|");
  key.append(db_name);
  key.append(".");
  key.append(name);
}

inline
Backup_info::Dep_node::Dep_node(const Dep_node &n)
 :Sql_alloc(n), next(n.next), obj(n.obj)
{
  key.copy(n.key);
}

inline
void Backup_info::Dep_node::free(void *record)
{
  ((Dep_node*)record)->~Dep_node();
}

inline
uchar* 
Backup_info::Dep_node::get_key(const uchar *record,
                              size_t *key_length,
                              my_bool) // not_used __attribute__((unused)))
{
  Dep_node *n= (Dep_node*)record;
  if (key_length)
    *key_length= n->key.length();
  return (uchar*)n->key.ptr();
}


/**
  Create @c Backup_info instance and prepare it for populating with objects.
  
  @param[in] log     A logger used to report errors
  @param[in] thd     THD handle

  Snapshots created by the built-in backup engines are added to @c snapshots
  list to be used in the backup engine selection algorithm in 
  @c find_backup_engine().
 */
Backup_info::Backup_info(backup::Logger &log, THD *thd)
  :m_log(log), m_thd(thd), m_state(Backup_info::ERROR), native_snapshots(8),
   m_dep_list(NULL), m_dep_end(NULL), 
   m_srout_end(NULL), m_view_end(NULL), m_trigger_end(NULL), m_event_end(NULL)
{
  using namespace backup;

  Snapshot_info *snap;

  bzero(m_snap, sizeof(m_snap));

  if (hash_init(&ts_hash, &::my_charset_bin, 16, 0, 0,
                Ts_hash_node::get_key, Ts_hash_node::free, MYF(0))
      ||
      hash_init(&dep_hash, &::my_charset_bin, 16, 0, 0,
                Dep_node::get_key, Dep_node::free, MYF(0)))
  {
    // Allocation failed. Error has been reported, but not logged to backup logs
    m_log.log_error(ER_OUT_OF_RESOURCES);
    return;
  }

  /* 
    Create nodata, default, and CS snapshot objects and add them to the 
    snapshots list. Note that the default snapshot should be the last 
    element on that list, as a "catch all" entry. 
   */

  snap= new Nodata_snapshot(m_log);             // logs errors
  if (!snap)
  {
    m_log.report_error(ER_OUT_OF_RESOURCES);
    return;
  }

  if (!snap->is_valid())
    return;    // Error has been logged by Nodata_snapshot constructor

  if (snapshots.push_back(snap))
  {
    // Allocation failed. Error has been reported, but not logged to backup logs
    m_log.log_error(ER_OUT_OF_RESOURCES);
    return;
  }

  snap= new CS_snapshot(m_log);                 // logs errors
  if (!snap)
  {
    m_log.report_error(ER_OUT_OF_RESOURCES);
    return;
  }

  if (!snap->is_valid())
    return;        // Error has been logged by CS_snapshot constructor

  if (snapshots.push_back(snap))
  {
    // Allocation failed. Error has been reported, but not logged to backup logs
    m_log.log_error(ER_OUT_OF_RESOURCES);
    return;                                   // Error has been logged
  }

  snap= new Default_snapshot(m_log);            // logs errors
  if (!snap)
  {
    m_log.report_error(ER_OUT_OF_RESOURCES);
    return;
  }

  if (!snap->is_valid())
    return;  // Error has been logged by Default_snapshot constructor

  if (snapshots.push_back(snap))
  {
    // Allocation failed. Error has been reported, but not logged to backup logs
    m_log.log_error(ER_OUT_OF_RESOURCES);
    return;                                   // Error has been logged
  }

  m_state= CREATED;
}

Backup_info::~Backup_info()
{
  using namespace backup;

  close();

  // delete Snapshot_info instances.

  Snapshot_info *snap;
  List_iterator<Snapshot_info> it(snapshots);

  while ((snap= it++))
    delete snap;

  hash_free(&ts_hash);  
  hash_free(&dep_hash);
}

/**
  Close @c Backup_info object after populating it with items.

  After this call the @c Backup_info object is ready for use as a catalogue
  for backup stream functions such as @c bstream_wr_preamble().
 */
int Backup_info::close()
{
  if (!is_valid())
    return ERROR;

  if (m_state == CLOSED)
    return 0;

  // report backup drivers used in the image
  
  for (ushort n=0; n < snap_count(); ++n)
    m_log.report_driver(m_snap[n]->name());
  
  m_state= CLOSED;
  return 0;
}  

/**
  Add tablespace to backup catalogue.

  @param[in]	obj		sever object representing the tablespace
  
  If tablespace is already present in the catalogue, the existing catalogue entry
  is returned. Otherwise a new entry is created and tablespace info stored in it.
  
  @return Pointer to (the new or existing) catalogue entry holding info about the
  tablespace.  
 */ 
backup::Image_info::Ts* Backup_info::add_ts(obs::Obj *obj)
{
  const String *name;

  DBUG_ASSERT(obj);
  name= obj->get_name();
  DBUG_ASSERT(name);

  /* 
    Check if tablespace with that name is already present in the catalogue using
    ts_hash.
  */

  Ts_hash_node n0(name);
  size_t klen= 0;
  uchar  *key= Ts_hash_node::get_key((const uchar*)&n0, &klen, TRUE);

  Ts_hash_node *n1= (Ts_hash_node*) hash_search(&ts_hash, key, klen);

  // if tablespace was found, return the catalogue entry stored in the hash
  if (n1)
    return n1->it;

  // otherwise create a new catalogue entry

  ulong pos= ts_count();

  Ts *ts= Image_info::add_ts(*name, pos);

  if (!ts)
  {
    m_log.report_error(ER_BACKUP_CATALOG_ADD_TS, name);
    return NULL;
  }

  // store pointer to the server object instance

  ts->m_obj_ptr= obj;

  // add new entry to ts_hash

  n1= new Ts_hash_node(n0);

  if (!n1)
  {
    m_log.report_error(ER_OUT_OF_RESOURCES);
    return NULL;
  }

  n1->it= ts;
  my_hash_insert(&ts_hash, (uchar*)n1);

  return ts;  
}

/**
  Select database object for backup.
  
  The object is added to the backup catalogue as an instance of 
  @c Image_info::Db class. A pointer to the obj::Obj instance is saved there for
  later usage.
  
  @returns Pointer to the @c Image_info::Db instance or NULL if database could
  not be added.
 */ 
backup::Image_info::Db* Backup_info::add_db(obs::Obj *obj)
{
  ulong pos= db_count();
  
  DBUG_ASSERT(obj);

  const ::String *name= obj->get_name();
  
  DBUG_ASSERT(name);  

  Db *db= Image_info::add_db(*name, pos);
  
  if (!db)
  {
    m_log.report_error(ER_BACKUP_CATALOG_ADD_DB, name->ptr());
    return NULL;
  }

  db->m_obj_ptr= obj;

  return db;  
}

/**
  Select given databases for backup.

  @param[in]  list of databases to be backed-up

  For each database, all objects stored in that database are also added to
  the image.

  @returns 0 on success, error code otherwise.
 */
int Backup_info::add_dbs(List< ::LEX_STRING > &dbs)
{
  using namespace obs;

  List_iterator< ::LEX_STRING > it(dbs);
  ::LEX_STRING *s;
  String unknown_dbs; // comma separated list of databases which don't exist

  while ((s= it++))
  {
    backup::String db_name(*s);

    // Ignore the database if it has already been inserted into the catalogue.    
    if (has_db(db_name))
      continue;
    
    if (is_internal_db_name(&db_name))
    {
      m_log.report_error(ER_BACKUP_CANNOT_INCLUDE_DB, db_name.c_ptr());
      goto error;
    }
    
    obs::Obj *obj= get_database(&db_name); // reports errors

    if (obj && !check_db_existence(&db_name))
    {    
      if (!unknown_dbs.is_empty()) // we just compose unknown_dbs list
      {
        delete obj;
        continue;
      }
      
      Db *db= add_db(obj);  // reports errors

      if (!db)
      {
        delete obj;
        goto error;
      }

      if (add_db_items(*db))  // reports errors
        goto error;
    }
    else if (obj)
    {
      if (!unknown_dbs.is_empty())
        unknown_dbs.append(",");
      unknown_dbs.append(*obj->get_name());
      
      delete obj;
    }
    else
      goto error; // error was reported in get_database()
  }

  if (!unknown_dbs.is_empty())
  {
    m_log.report_error(ER_BAD_DB_ERROR, unknown_dbs.c_ptr());
    goto error;
  }

  return 0;

 error:

  m_state= ERROR;
  return backup::ERROR;
}

/**
  Select all existing databases for backup.

  For each database, all objects stored in that database are also added to
  the image. The internal databases are skipped.

  @returns 0 on success, error code otherwise.
*/
int Backup_info::add_all_dbs()
{
  using namespace obs;

  int res= 0;
  Obj_iterator *dbit= get_databases(m_thd);
  
  if (!dbit)
  {
    m_log.report_error(ER_BACKUP_LIST_DBS);
    return ERROR;
  }
  
  obs::Obj *obj;
  
  while ((obj= dbit->next()))
  {
    // skip internal databases
    if (is_internal_db_name(obj->get_name()))
    {
      DBUG_PRINT("backup",(" Skipping internal database %s", 
                           obj->get_name()->ptr()));
      delete obj;
      continue;
    }

    DBUG_PRINT("backup", (" Found database %s", obj->get_name()->ptr()));

    Db *db= add_db(obj);  // reports errors

    if (!db)
    {
      res= ERROR;
      delete obj;
      goto finish;
    }

    if (add_db_items(*db))  // reports errors
    {
      res= ERROR;
      goto finish;
    }
  }

  DBUG_PRINT("backup", ("No more databases in I_S"));

 finish:

  delete dbit;

  if (res)
    m_state= ERROR;

  return res;
}


/**
  Store in Backup_image all objects enumerated by the iterator.

  @param[in]  db  database to which objects belong - this database must already
                  be in the catalogue
  @param[in] type type of objects (only objects of the same type can be added)
  @param[in] it   iterator enumerationg objects to be added

  @returns 0 on success, error code otherwise.
 */
int Backup_info::add_objects(Db &db, const obj_type type, obs::Obj_iterator &it)
{
  obs::Obj *obj;
  
  while ((obj= it.next()))
    if (!add_db_object(db, type, obj)) // reports errors
    {
      delete obj;
      return ERROR;
    }

  return 0;
}

/**
  Add to image all objects belonging to a given database.

  @returns 0 on success, error code otherwise.
 */
int Backup_info::add_db_items(Db &db)
{
  using namespace obs;

  // Add tables.

  Obj_iterator *it= get_db_tables(m_thd, &db.name());

  if (!it)
  {
    m_log.report_error(ER_BACKUP_LIST_DB_TABLES, db.name().ptr());
    return ERROR;
  }
  
  int res= 0;
  obs::Obj *obj= NULL;

  while ((obj= it->next()))
  {
    DBUG_PRINT("backup", ("Found table %s for database %s",
                           obj->get_name()->ptr(), db.name().ptr()));

    /*
      add_table() method selects/creates a snapshot to which this table is added.
      The backup engine is choosen in Backup_info::find_backup_engine() method.
    */
    Table *tbl= add_table(db, obj); // reports errors

    if (!tbl)
    {
      delete obj;
      goto error;
    }

    // If this table uses a tablespace, add this tablespace to the catalogue.

    obj= get_tablespace_for_table(m_thd, &db.name(), &tbl->name());

    if (obj)
    {
      Ts *ts= add_ts(obj); // reports errors

      if (!ts)
      {
        delete obj;
        goto error;
      }
    }
  }

  // Add other objects.

  delete it;  
  it= get_db_stored_procedures(m_thd, &db.name());
  
  if (!it)
  {
    m_log.report_error(ER_BACKUP_LIST_DB_SROUT, db.name().ptr());
    goto error;
  }
  
  if (add_objects(db, BSTREAM_IT_SPROC, *it))
    goto error;

  delete it;
  it= get_db_stored_functions(m_thd, &db.name());

  if (!it)
  {
    m_log.report_error(ER_BACKUP_LIST_DB_SROUT, db.name().ptr());
    goto error;
  }
  
  if (add_objects(db, BSTREAM_IT_SFUNC, *it))
    goto error;

  delete it;
  it= get_db_views(m_thd, &db.name());

  if (!it)
  {
    m_log.report_error(ER_BACKUP_LIST_DB_VIEWS, db.name().ptr());
    goto error;
  }
  
  if (add_objects(db, BSTREAM_IT_VIEW, *it))
    goto error;

  delete it;
  it= get_db_events(m_thd, &db.name());

  if (!it)
  {
    m_log.report_error(ER_BACKUP_LIST_DB_EVENTS, db.name().ptr());
    goto error;
  }
  
  if (add_objects(db, BSTREAM_IT_EVENT, *it))
    goto error;
  
  delete it;
  it= get_db_triggers(m_thd, &db.name());

  if (!it)
  {
    m_log.report_error(ER_BACKUP_LIST_DB_TRIGGERS, db.name().ptr());
    goto error;
  }
  
  if (add_objects(db, BSTREAM_IT_TRIGGER, *it))
    goto error;
  
  delete it;
  it= get_all_db_grants(m_thd, &db.name());

  if (!it)
  {
    m_log.report_error(ER_BACKUP_LIST_DB_PRIV, db.name().ptr());
    goto error;
  }

  if (add_objects(db, BSTREAM_IT_PRIVILEGE, *it))
    goto error;

  goto finish;

 error:

  res= res ? res : ERROR;
  m_state= ERROR;
  
 finish:

  delete it;
  return res;
}

namespace {

/**
  Implementation of @c Table_ref which gets table identity from a server
  object instance - to be used in @c Backup_info::add_table().
 */ 
class Tbl: public backup::Table_ref
{
 public:

   Tbl(obs::Obj *obj) :backup::Table_ref(*obj->get_db_name(), *obj->get_name())
   {}

   ~Tbl()
   {}
};

} // anonymous namespace

/**
  Select table object for backup.

  @param[in]  dbi   database to which the table belongs
  @param[in]  obj   table object

  The object is added to the backup image's catalogue as an instance of
  @c Image_info::Table class. A pointer to the obj::Obj instance is saved for 
  later usage. This method picks the best available backup engine for the table 
  using @c find_backup_engine() method.

  @todo Correctly handle temporary tables.

  @returns Pointer to the @c Image_info::Table class instance or NULL if table
  could not be added.
*/
backup::Image_info::Table* Backup_info::add_table(Db &dbi, obs::Obj *obj)
{
  Table *tbl= NULL;
  
  DBUG_ASSERT(obj);

  Tbl t(obj);
  // TODO: skip table if it is a tmp one
  
  backup::Snapshot_info *snap= find_backup_engine(t); // reports errors

  if (!snap)
    return NULL;

  // add table to the catalogue

  ulong pos= snap->table_count();
  
  tbl= Image_info::add_table(dbi, t.name(), *snap, pos);
  
  if (!tbl)
  {
    m_log.report_error(ER_BACKUP_CATALOG_ADD_TABLE, 
                       dbi.name().ptr(), t.name().ptr());
    return NULL;
  }

  tbl->m_obj_ptr= obj;

  DBUG_PRINT("backup",(" table %s backed-up with %s engine (snapshot %d)",
                      t.name().ptr(), snap->name(), snap->m_num));
  return tbl;
}

/**
  For all views on which the given one depends directly or indirectly, add 
  placeholders to the dependency list ensuring correct order among them.

  The main view being processed in this function should be added to the 
  dependency list after all the placeholders created here. This way if one of 
  the views on which the given one depends is added to the list later, it will 
  be placed at the correct position indicated by the placeholder.

  @param[in]  obj   server object for the view to be processed

  A recursive algorithm is used to insert placeholders in correct order.
  That is, for each base view @c bv of the given one, @c add_view_deps(bv) 
  is called to insert all dependencies of @c bv before @c bv itself is appended
  to the list.
  
  Function @c get_dep_node() used to create a node to be inserted into the 
  dependency list detects if the node was already created earlier. This ensures
  correct behaviour of the algorithm even if the same view is visited several 
  times during the depth-first walk of the dependency graph performed by the 
  recursive algorithm. If it is detected that a node for a given view already
  exists then this view is not processed for the second time.  

  This also ensures termination of the algorithm even when there are
  circular dependencies. Suppose that view @c v has itself as an (indirect) 
  dependency. When processing @c v, a node will be created for it first 
  and then its dependencies will be processed. When add_view_deps() comes across 
  @c v for the second time it will see that a corresponding mode already 
  exists and thus will break the recursion.

  @return Non-zero value if an error happened.
 */ 
int Backup_info::add_view_deps(obs::Obj &obj)
{
  const ::String *name= obj.get_name();
  const ::String *db_name= obj.get_db_name();

  DBUG_ASSERT(name); 
  DBUG_ASSERT(db_name); 
  
  // Get an iterator to iterate over base views of the given one.

  obs::Obj_iterator *it= obs::get_view_base_views(m_thd, db_name, name);

  if (!it)
    return ERROR;

  /* 
    Iterate over base views and for each of them add it and its dependencies 
    to the dependency list (first its dependecies, then the base view).
  */

  obs::Obj *bv; 

  while ((bv= it->next()))
  {
    Dep_node *n= NULL;
    const ::String *name= bv->get_name();
    const ::String *db_name= bv->get_db_name();

    DBUG_ASSERT(name); 
    DBUG_ASSERT(db_name); 

    // Locate or create a dependency list node for the base view.

    int res= get_dep_node(*db_name, *name, BSTREAM_IT_VIEW, n);

    if (res == get_dep_node_res::ERROR)
      goto error;

    DBUG_ASSERT(n);

    /*
      If a node for this view already exists, the view has been processed 
      (or is being processed now). Hence we skip it and continue with other
      base views.
     */ 
    if (res == get_dep_node_res::EXISTING_NODE)
    {
      delete bv; // Need to delete the instance returned by it->next().
      continue;
    }

    // Recursively add all dependencies of bv to the list.

    if (add_view_deps(*bv))
      goto error;

    /* 
      Now add bv itself to the list (we keep in n a pointer to the dep. list
      node obtained earlier).
     */
    if (add_to_dep_list(BSTREAM_IT_VIEW, n))
      goto error;      

    delete bv; // Server object instance is not needed any more.
  }

  delete it;
  return 0;

error:

  delete bv;
  delete it;

  return ERROR;
}

/**
  Select a per database object for backup.

  This method is used for objects other than tables - tables are handled
  by @c add_table(). The object is added at first available position. Pointer
  to @c obj is stored for later usage.

  @param[in] db   object's database - must already be in the catalogue
  @param[in] type type of the object
  @param[in] obj  the object

  The object is added both to the dependency list with @c
  add_to_dep_list() method and to the catalogue. If it is a view, its
  dependencies are handled first using @c add_view_deps().

  @returns Pointer to @c Image_info::Dbobj instance storing information 
  about the object or NULL in case of error.  
 */
backup::Image_info::Dbobj* 
Backup_info::add_db_object(Db &db, const obj_type type, obs::Obj *obj)
{
  int error= 0;
  ulong pos= db.obj_count();

  DBUG_ASSERT(obj);
  String *name= (String *)obj->get_name();
  DBUG_ASSERT(name);

  switch (type) {

  // Databases and tables should not be passed to this function.  
  case BSTREAM_IT_DB:     DBUG_ASSERT(FALSE); break;
  case BSTREAM_IT_TABLE:  DBUG_ASSERT(FALSE); break;

  case BSTREAM_IT_VIEW:   error= ER_BACKUP_CATALOG_ADD_VIEW; break;
  case BSTREAM_IT_SPROC:  error= ER_BACKUP_CATALOG_ADD_SROUT; break;
  case BSTREAM_IT_SFUNC:  error= ER_BACKUP_CATALOG_ADD_SROUT; break;
  case BSTREAM_IT_EVENT:  error= ER_BACKUP_CATALOG_ADD_EVENT; break;
  case BSTREAM_IT_TRIGGER: error= ER_BACKUP_CATALOG_ADD_TRIGGER; break;
  case BSTREAM_IT_PRIVILEGE: error= ER_BACKUP_CATALOG_ADD_PRIV; break;
  
  // Only known types of objects should be added to the catalogue.
  default: DBUG_ASSERT(FALSE);

  }

  /*
    Generate a unique name for the privilege (grant) objects.
    Note: this name does not alter the mechanics of the
          grant objects in si_objects.cc
  */
  if (type == BSTREAM_IT_PRIVILEGE)
  {
    String new_name;
    char buff[10];
    sprintf(buff, UNIQUE_PRIV_KEY_FORMAT, pos);
    new_name.append(*name);
    new_name.append(" ");
    new_name.append(buff);
    name->copy(new_name);
  }

  /* 
    Add new object to the dependency list. If it is a view, add its
    dependencies first.
   */

  Dep_node *n= NULL; /* Note: set to NULL to make sure that get_dep_node() has 
                        set the pointer. */ 

  // Get a dep. list node for the object.  

  int res= get_dep_node(db.name(), *name, type, n);
  
  if (res == get_dep_node_res::ERROR)
  {
    m_log.report_error(error, db.name().ptr(), name->ptr());
    return NULL;
  }

  /*
    If a new node was created, it must be added to the dependency list with
    add_to_dep_list(). However, if the object is a view, we must first add 
    placeholders for all its dependencies which is done using add_view_deps().
   */ 

  if (res == get_dep_node_res::NEW_NODE)
  {
    if (type == BSTREAM_IT_VIEW)
      if (add_view_deps(*obj))
      {
        m_log.report_error(error, db.name().ptr(), name->ptr());
        return NULL;
      } 

    add_to_dep_list(type, n);
  } 

  /*
    The object has now been added to the dependancy list. If it is a
    view, all dependant objects have also been successfully added to
    the dependency list. The object can now be added to the cataloge
    and then be linked to from the node in the dep list. Adding to dep
    list before adding to catalogue ensures that an object will not be
    added to catalogue if there are problems with it's dependant
    objects.
   */

  Dbobj *o= Image_info::add_db_object(db, type, *name, pos);
 
  if (!o)
  {
    m_log.report_error(error, db.name().ptr(), name->ptr());
    return NULL;
  }

  o->m_obj_ptr= obj;

  /* 
    Store a pointer to the catalogue item in the dep. list node. If this node
    was a placeholder inserted into the list before, now it will be filled with
    the object we are adding to the catalogue.
   */

  DBUG_ASSERT(n);
  n->obj= o;  

  DBUG_PRINT("backup",("Added object %s of type %d from database %s (pos=%lu)",
                       name->ptr(), type, db.name().ptr(), pos));
  return o;
}

/**
  Find or create a dependency list node for an object with a given name.
  
  @param[in]  db_name  name of the database to which this object belongs
  @param[in]  name     name of the object
  @param[out] node     pointer to the created or located node
  
  All nodes created using this function are keept inside @c dep_node HASH
  indexed by <db_name, name> pairs. This is used to detect that a node for
  a given object already exists. All created nodes are deleted when Backup_info
  instance is destroyed.

  The node created by this function is not placed on the dependency list. This
  must be done explicitly using @c add_to_dep_list(). The node has @c obj member
  set to NULL which means that it can be used as an empty placeholder in the 
  dependency list.

  @return A code indicating whether the node was found or created.
  @retval NEW_NODE      a new node has been created
  @retval EXISTING_NODE a node for this object was created before and @c node
                        contains a pointer to it
  @retval ERROR         it was not possible to create or locate the node
 */ 
int Backup_info::get_dep_node(const ::String &db_name, 
                              const ::String &name, 
                              const obj_type type,
                              Dep_node* &node)
{
  Dep_node n(db_name, name, type);
  size_t klen;
  uchar  *key= Dep_node::get_key((const uchar*)&n, &klen, TRUE);

  node= (Dep_node*) hash_search(&dep_hash, key, klen);

  // if we have found node in the hash there is nothing more to do
  if (node)
    return get_dep_node_res::EXISTING_NODE;
  
  /*
    Otherwise insert node into the hash and return.
   */

  node= new (&mem_root) Dep_node(n);

  if (!node)
    return get_dep_node_res::ERROR;

  return my_hash_insert(&dep_hash, (uchar*)node) ? 
          get_dep_node_res::ERROR : get_dep_node_res::NEW_NODE;
}

/**
  Append node to the correct section of the dependency list, based on the
  type of the object.

  @param[in]  type   type of the object indicating the section of the list 
                     to which node will be appended
  @param[in]  node   points at the node to be appended

  The node is appended at the end of the corresponding section of the
  dependency list. Pointers m_dep_list, m_end_dep and the ones indicating end of
  each section are updated as necessary.
  
  A node added to the list is just a placeholder for an object from backup 
  catalogue. To insert such object into the list a pointer to the corresponding
  @c Dbobj instance should be stored in the @c obj member of a list node.
 
  @return Non-zero value in case of error. Currently should always succeed. 
 */ 
int Backup_info::add_to_dep_list(const obj_type type, Dep_node *node)
{
  Dep_node* *end= NULL;

  DBUG_ASSERT(node);

  /*
    Set end to point at m_srout_end, m_trigger_end or m_view_end depending on 
    the type of the item.
    
    If the corresponding section is empty, the *end pointer is set up to point
    at the node after which this section should start, or to NULL if section
    should be at the beginning of the dependency list.
    
    After inserting the new node it becomes the last node in the section so
    the pointer is updated to point at it.
   */ 

  switch (type) {

  case BSTREAM_IT_SPROC:
  case BSTREAM_IT_SFUNC:
    end= &m_srout_end;
  break; 

  case BSTREAM_IT_VIEW:  
    end= &m_view_end;
    if (!m_view_end)
      m_view_end= m_srout_end; 
  break;

  case BSTREAM_IT_TRIGGER:
    end= &m_trigger_end;
    if (!m_trigger_end)
      m_trigger_end= m_view_end ? m_view_end : m_srout_end;
  break;
  
  case BSTREAM_IT_EVENT: 
    end= &m_event_end;
    if (!m_event_end)
      m_event_end= m_trigger_end ? m_trigger_end : m_view_end ? m_view_end : m_srout_end;
  break;

  case BSTREAM_IT_PRIVILEGE:
    end= &m_dep_end;
  break;
   
  default: DBUG_ASSERT(FALSE); // only known object types should be added   

  }

  DBUG_ASSERT(end); // end should point now at one of m_*_end pointers

  /*
    The new node should be inserted after the node pointed by end or at
    the begginging of the list if end == NULL. 
   */

  if (*end)
  {
    node->next= (*end)->next;
    (*end)->next= node;
  }
  else
  {
    node->next= m_dep_list;
    m_dep_list= node;
  }

  /*
    Update m_dep_end pointer if appending to the end of the dependency list.
    
    There are two cases:
    
    - either the list is empty in which case both m_dep_end and *end are NULL
    - or it is not empty and *end points at the last node in the list.
    
    In either case *end equals m_dep_end.
   */ 

  if (*end == m_dep_end)
    m_dep_end= node;

  *end= node;

  return 0;
}


/**
  Used to iterate over all global objects for which we store information in the
  neta-data section of backup image.

  Currently only global objects handled are tablespaces and databases.
 */
class Backup_info::Global_iterator
 : public backup::Image_info::Iterator
{
  /**
    Indicates whether tablespaces or databases are being currently enumerated.
   */ 
  enum { TABLESPACES, DATABASES, DONE } mode;

  Iterator *m_it; ///< Points at the currently used iterator.
  Obj *m_obj;         ///< Points at next object to be returned by this iterator.

 public:

  Global_iterator(const Backup_info&);

  int init();

 private:

  Obj* get_ptr() const;
  bool next();
};

inline
Backup_info::Global_iterator::Global_iterator(const Backup_info &info)
 :Iterator(info), mode(TABLESPACES), m_it(NULL), m_obj(NULL)
{}


inline
int Backup_info::Global_iterator::init()
{
  m_it= m_info.get_tablespaces();
  if (!m_it)
  {
    const Backup_info* info= static_cast<const Backup_info*>(&m_info);
    return info->m_log.report_error(ER_OUT_OF_RESOURCES);
  }
  next();                                       // Never errors

  return 0;
}

inline
backup::Image_info::Obj*
Backup_info::Global_iterator::get_ptr() const
{
  return m_obj;
}

inline
bool
Backup_info::Global_iterator::next()
{
  if (mode == DONE)
    return FALSE;

  DBUG_ASSERT(m_it);

  // get next object from the current iterator
  m_obj= (*m_it)++;

  if (m_obj)
    return TRUE;

  /*
    If the current iterator has finished (m_obj == NULL) then, depending on
    the mode, either switch to the next iterator or mark end of the sequence.
  */

  delete m_it;

  switch (mode) {

  case TABLESPACES:

    // We have finished enumerating tablespaces, move on to databases.
    mode= DATABASES;
    m_it= m_info.get_dbs();
    if (!m_it)
    {
      const Backup_info* info= static_cast<const Backup_info*>(&m_info);
      info->m_log.report_error(ER_OUT_OF_RESOURCES);
      mode= DONE;
      return FALSE;
    }

    m_obj= (*m_it)++;
    return m_obj != NULL;

  case DATABASES:

    // We have finished enumerating databases - nothing more to do.
    mode= DONE;

  case DONE:

    break;
  }

  return FALSE;
}

/**
  Used to iterate over all per database objects, except tables.

  This iterator uses the dependency list maintained inside Backup_info
  instance to list objects in a dependency-respecting order.
 */ 
class Backup_info::Perdb_iterator : public backup::Image_info::Iterator
{
  Dep_node *ptr;

 public:

  Perdb_iterator(const Backup_info&);

 private:

  Obj* get_ptr() const;
  bool next();
};

inline
Backup_info::Perdb_iterator::Perdb_iterator(const Backup_info &info)
 :Iterator(info), ptr(info.m_dep_list)
{
  // Find first non-empty node in the dependency list.

  while (ptr && !ptr->obj)
    ptr= ptr->next;
}

/// Implementation of @c Image_info::Iterator virtual method.
inline
backup::Image_info::Obj* Backup_info::Perdb_iterator::get_ptr() const
{
  return ptr ? ptr->obj : NULL;
}

/// Implementation of @c Image_info::Iterator virtual method.
inline
bool Backup_info::Perdb_iterator::next()
{
  // Return FALSE if we are at the end of dependency list.

  if (!ptr)
    return FALSE;

  // Otherwise move ptr to the next non-empty node on the list.

  do {
    ptr= ptr->next;
  } while (ptr && !ptr->obj);

  return ptr != NULL;
}


/// Wrapper to return global iterator.
backup::Image_info::Iterator* Backup_info::get_global() const
{
  Global_iterator* it = new Global_iterator(*this);
  if (it == NULL) 
  {
    m_log.report_error(ER_OUT_OF_RESOURCES);
    return NULL;
  }    
  if (it->init())                               // Error has been logged
  {
    return NULL;
  }

  return it;
}

/// Wrapper to return iterator for per-database objects.
backup::Image_info::Iterator* Backup_info::get_perdb()  const
{
  Perdb_iterator* it = new Perdb_iterator(*this);
  if (it == NULL) 
  {
    m_log.report_error(ER_OUT_OF_RESOURCES);
    return NULL;
  }    
  if (it->init())                               // Error has been logged
  {
    return NULL;
  }

  return it;
}
