/**
  @file

  Implementation of the backup kernel API.

  @todo Add more error messages.
  @todo Use internal table name representation when passing tables to
        backup/restore drivers.
  @todo Handle other types of meta-data in Backup_info methods.
  @todo Handle item dependencies when adding new items.
  @todo Handle other kinds of backup locations (far future).
 */

#include "../mysql_priv.h"

#include "backup_kernel.h"
#include "logger.h"
#include "stream.h"
#include "debug.h"
#include "be_native.h"
#include "be_default.h"
#include "be_snapshot.h"
#include "ddl_blocker.h"
#include "backup_progress.h"

/*
  How to use backup kernel API to perform backup and restore operations
  =====================================================================
  
  To perform backup or restore operation an appropriate context must be created.
  This involves creating required resources and correctly setting up the server.
  When operation is completed or interrupted, the context must be destroyed and
  all preparations reversed.
  
  All this is accomplished by creating an instance of Backup_crate_ctx class and
  then using its methods. When the instance is destroyed, the required clean-up
  is performed.
  
  This is how backup is performed using the context object:

  {
  
   Backup_restore_ctx context(thd); // create context instance
   Backup_info *info= context.prepare_for_backup(location); // prepare for backup
  
   // select objects to backup
   info->add_all_dbs();
   or
   info->add_dbs(<list of db names>);
  
   info->close(); // indicate that selection is done
  
   context.do_backup(); // perform backup
   
   context.close(); // explicit clean-up
  
  } // if code jumps here, context destructor will do the clean-up automatically

  
  Similar code will be used for restore (bit simpler as we don't support 
  selective restores yet):

  {
  
   Backup_restore_ctx context(thd); // create context instance
   Restore_info *info= context.prepare_for_restore(location); // prepare for restore
  
   context.do_restore(); // perform restore
   
   context.close(); // explicit clean-up
  
  } // if code jumps here, context destructor will do the clean-up automatically

 */ 


/** 
  Global Initialization for online backup system.
 
  @note This function is called when server loads its plugins.
 */
int backup_init()
{
  pthread_mutex_init(&Backup_restore_ctx::run_lock, MY_MUTEX_INIT_FAST);
  return 0;
}

/**
  Global clean-up for online backup system.
  
  @note This function is called when server shuts down its plugins.
 */
void backup_shutdown()
{
  pthread_mutex_destroy(&Backup_restore_ctx::run_lock);
}

/*
  Forward declarations of functions used for sending response from BACKUP/RESTORE
  statement.
 */ 
static int send_error(Backup_restore_ctx &context, int error_code, ...);
static int send_reply(Backup_restore_ctx &context);


/**
  Call backup kernel API to execute backup related SQL statement.

  @param lex  results of parsing the statement.

  @note This function sends response to the client (ok, result set or error).
 */

int
execute_backup_command(THD *thd, LEX *lex)
{
  int res= 0;
  
  DBUG_ENTER("execute_backup_command");
  DBUG_ASSERT(thd && lex);

  BACKUP_BREAKPOINT("backup_command");

  using namespace backup;

  Backup_restore_ctx context(thd);
  
  if (!context.is_valid())
  {
    context.report_error(ER_BACKUP_UNKNOWN_ERROR);
    DBUG_RETURN(send_error(context,ER_BACKUP_UNKNOWN_ERROR));
  }

  switch (lex->sql_command) {

  case SQLCOM_SHOW_ARCHIVE:
    
    my_error(ER_NOT_ALLOWED_COMMAND,MYF(0));
    DBUG_RETURN(ER_NOT_ALLOWED_COMMAND);

  case SQLCOM_BACKUP:
  {
    // prepare for backup operation
    
    Backup_info *info= context.prepare_for_backup(lex->backup_dir); // reports errors

    if (!info || !info->is_valid())
      DBUG_RETURN(send_error(context,ER_BACKUP_BACKUP_PREPARE));

    BACKUP_BREAKPOINT("bp_running_state");

    // select objects to backup

    if (lex->db_list.is_empty())
    {
      context.write_message(log_level::INFO,"Backing up all databases");
      res= info->add_all_dbs(); // backup all databases
    }
    else
    {
      context.write_message(log_level::INFO,"Backing up selected databases");
      res= info->add_dbs(lex->db_list); // backup databases specified by user
    }

    info->close(); // close catalogue after filling it with objects to backup

    if (res || !info->is_valid())
      DBUG_RETURN(send_error(context,ER_BACKUP_UNKNOWN_ERROR));

    if (info->db_count() == 0)
    {
      context.report_error(ER_BACKUP_NOTHING_TO_BACKUP);
      DBUG_RETURN(send_error(context,ER_BACKUP_NOTHING_TO_BACKUP));
    }

    // perform backup

    res= context.do_backup();
 
    if (res)
      DBUG_RETURN(send_error(context,ER_BACKUP_BACKUP));

    BACKUP_BREAKPOINT("bp_complete_state");
    break;
  }

  case SQLCOM_RESTORE:
  {
    Restore_info *info= context.prepare_for_restore(lex->backup_dir);
    
    if (!info || !info->is_valid())
      DBUG_RETURN(send_error(context,ER_BACKUP_RESTORE_PREPARE));
    
    BACKUP_BREAKPOINT("bp_running_state");

    res= context.do_restore();      

    if (res)
      DBUG_RETURN(send_error(context,ER_BACKUP_RESTORE));
    
    break;
  }

  default:
     /*
       execute_backup_command() should be called with correct command id
       from the parser. If not, we fail on this assertion.
      */
     DBUG_ASSERT(FALSE);

  } // switch(lex->sql_command)

  if (context.close())
    DBUG_RETURN(send_error(context,ER_BACKUP_UNKNOWN_ERROR));

  // All seems OK - send positive reply to client

  DBUG_RETURN(send_reply(context));
}

namespace backup {

/**
  This calss provides memory allocation services for backup stream library.

  An instance of this class is created during preparations for backup/restore
  operation. When it is deleted, all allocated memory is freed.
*/
class Mem_allocator
{
 public:

  Mem_allocator();
  ~Mem_allocator();

  void* alloc(size_t);
  void  free(void*);

 private:

  struct node;
  node *first;  ///< Pointer to the first segment in the list.
};


} // backup namespace


/*************************************************

   Implementation of Backup_restore_ctx class

 *************************************************/

// static members

bool Backup_restore_ctx::is_running= FALSE;
pthread_mutex_t Backup_restore_ctx::run_lock;
backup::Mem_allocator *Backup_restore_ctx::mem_alloc= NULL;

// access the DDL blocker instance
extern DDL_blocker_class *DDL_blocker;

Backup_restore_ctx::Backup_restore_ctx(THD *thd):
 m_state(CREATED), m_thd(thd), m_thd_options(thd->options),
 m_error(0), m_remove_loc(FALSE), m_stream(NULL), m_catalog(NULL)
{
  /*
    Check for progress tables.
  */
  if (check_ob_progress_tables(thd))
    m_error= ER_BACKUP_PROGRESS_TABLES;
}

Backup_restore_ctx::~Backup_restore_ctx()
{
  close();
  
  delete m_catalog;  
  delete m_stream;
}

/**
  Do preparations common to backup and restore operations.
  
  It is checked if another operation is in progress and if yes then
  error is reported. Otherwise the current operation is registered so that
  no other can be started. Also, a memory allocator for backup stream library
  is initialized.
 */ 
int Backup_restore_ctx::prepare(LEX_STRING location)
{
  if (m_error)
    return m_error;
  
  /*
    Check access for SUPER rights. If user does not have SUPER, fail with error.
  */
  if (check_global_access(m_thd, SUPER_ACL))
    report_error((m_error= ER_SPECIFIC_ACCESS_DENIED_ERROR), "SUPER");

  /*
    Check if another BACKUP/RESTORE is running and if not, register 
    this operation.
   */

  pthread_mutex_lock(&run_lock);

  if (!is_running)
    is_running= TRUE;
  else
    report_error((m_error= ER_BACKUP_RUNNING));

  pthread_mutex_unlock(&run_lock);

  if (m_error)
    return m_error;

  // check if location is valid (we assume it is a file path)

  bool bad_filename= (location.length == 0);
  
  /*
    On some systems certain file names are invalid. We use 
    check_if_legal_filename() function from mysys to detect this.
   */ 
#if defined(__WIN__) || defined(__EMX__)  

  bad_filename ||= check_if_legal_filename(location.str);
  
#endif

  if (bad_filename)
  {
    report_error((m_error= ER_BACKUP_UNKNOWN_ERROR),location.str);
    return m_error;
  }

  m_path= location.str;

  // create new instance of memory allocator for backup stream library

  using namespace backup;

  delete mem_alloc;
  mem_alloc= new Mem_allocator();

  if (!mem_alloc)
  {
    report_error((m_error= ER_BACKUP_UNKNOWN_ERROR));
    return m_error;
  }

  // Freeze all meta-data. 

  if (!DDL_blocker->block_DDL(m_thd))
  {
    report_error((m_error= ER_BACKUP_UNKNOWN_ERROR));
    return m_error;
  }

  return 0;
}

/**
  Prepare for backup operation.
  
  @param[in] location   path to the file where backup image should be stored
  
  @returns Pointer to a @c Backup_info instance which can be used for selecting
  which objects to backup. NULL if an error was detected.
  
  @note This function reports errors.
 */ 
Backup_info* Backup_restore_ctx::prepare_for_backup(LEX_STRING location)
{
  using namespace backup;
  
  if (m_error)
    return NULL;
  
  if (Logger::init(m_thd, BACKUP, location))
  {
    report_error((m_error= ER_BACKUP_UNKNOWN_ERROR));
    return NULL;
  }
  
  time_t when= my_time(0);
  report_start(when);
  
  /*
    Do preparations common to backup and restore operations.
   */ 
  if (prepare(location))
    return NULL;
  
  backup::String path(location);
  
  /*
    Open output stream.
   */

  OStream *s= new OStream(*this, path);
  
  if (!s)
  {
    report_error(ER_BACKUP_UNKNOWN_ERROR);
    return NULL;
  }
  
  if (!s->open())
    return NULL;

  m_stream= s;

  /*
    Create backup catalogue.
   */

  Backup_info *info= new Backup_info(*this); // reports errors

  if (!info)
  {
    report_error(ER_BACKUP_UNKNOWN_ERROR);
    return NULL;
  }

  if (!info->is_valid())
    return NULL;

  info->save_start_time(when);
  m_catalog= info;
  m_state= PREPARED_FOR_BACKUP;
  
  return info;
}

/**
  Prepare for restore operation.
  
  @param[in] location   path to the file where backup image is stored
  
  @returns Pointer to a @c Restore_info instance containing catalogue of the
  backup image (read from the image). NULL if errors were detected.
  
  @note This function reports errors.
 */ 
Restore_info* Backup_restore_ctx::prepare_for_restore(LEX_STRING location)
{
  using namespace backup;  

  if (m_error)
    return NULL;
  
  if (Logger::init(m_thd, RESTORE, location))
  {
    report_error((m_error= ER_BACKUP_UNKNOWN_ERROR));
    return NULL;
  }

  time_t when= my_time(0);  
  report_start(when);

  /*
    Do preparations common to backup and restore operations.
   */ 
  if (prepare(location))
    return NULL;
  
  /*
    Open input stream.
   */

  backup::String path(location);
  IStream *s= new IStream(*this, path);
  
  if (!s)
  {
    report_error((m_error= ER_BACKUP_UNKNOWN_ERROR));
    return NULL;
  }
  
  if (!s->open())
    return NULL;

  m_stream= s;

  /*
    Create restore catalogue.
   */

  Restore_info *info= new Restore_info(*this);  // reports errors

  if (!info)
  {
    report_error((m_error= ER_BACKUP_UNKNOWN_ERROR));
    return NULL;
  }

  if (!info->is_valid())
    return NULL;

  info->save_start_time(when);
  m_catalog= info;

  /*
    Read catalogue from input stream.
   */

  if (read_header(*info, *s))
  {
    report_error((m_error= ER_BACKUP_UNKNOWN_ERROR));
    return NULL;
  }

  if (s->next_chunk() != BSTREAM_OK)
  {
    report_error((m_error= ER_BACKUP_UNKNOWN_ERROR));
    return NULL;
  }

  if (read_catalog(*info, *s))
  {
    report_error((m_error= ER_BACKUP_UNKNOWN_ERROR));
    return NULL;
  }

  if (s->next_chunk() != BSTREAM_OK)
  {
    report_error((m_error= ER_BACKUP_UNKNOWN_ERROR));
    return NULL;
  }

  m_state= PREPARED_FOR_RESTORE;

  return info;
}

/**
  Destroy a backup/restore context.
  
  This should reverse all settings made when context was created and prepared.
  If it was requested, the backup/restore location is removed. Also, the backup
  stream memory allocator is shut down. Any other allocated resources are 
  deleted in the destructor.
  
  @returns 0 or error code if error was detected.
  
  @note This function reports errors.
 */ 
int Backup_restore_ctx::close()
{
  if (m_state == CLOSED)
    return 0;

  using namespace backup;

  time_t when= my_time(0);

  // unfreeze meta-data

  DDL_blocker->unblock_DDL();

  // restore thread options

  m_thd->options= m_thd_options;

  // close stream

  if (m_stream)
    m_stream->close();

  if (m_catalog)
    m_catalog->save_end_time(when);

  // destroy backup stream's memory allocator (this frees memory)

  delete mem_alloc;
  mem_alloc= NULL;
  
  // deregister this operation

  pthread_mutex_lock(&run_lock);
  is_running= FALSE;
  pthread_mutex_unlock(&run_lock);

  // remove the location, if asked for
  
  if (m_remove_loc)
  {
    int res= my_delete(m_path, MYF(0));

    /*
      Ignore ENOENT error since it is ok if the file doesn't exist.
     */
    if (res && my_errno != ENOENT)
    {
      report_error(ER_CANT_DELETE_FILE, m_path, my_errno);
      if (!m_error)
        m_error= ER_CANT_DELETE_FILE;
    }
  }

  // We report completion of the operation only if no errors were detected.

  if (!m_error)
    report_stop(when,TRUE);

  m_state= CLOSED;
  return m_error;
}

/**
  Create backup archive.
  
  @pre @c prepare_for_backup() method was called.
*/
int Backup_restore_ctx::do_backup()
{
  DBUG_ENTER("do_backup");

  // This function should not be called when context is not valid
  DBUG_ASSERT(is_valid());
  DBUG_ASSERT(m_state == PREPARED_FOR_BACKUP);
  DBUG_ASSERT(m_thd);
  DBUG_ASSERT(m_stream);
  DBUG_ASSERT(m_catalog);
  
  using namespace backup;

  OStream &s= *static_cast<OStream*>(m_stream);
  Backup_info &info= *static_cast<Backup_info*>(m_catalog);

  BACKUP_BREAKPOINT("backup_meta");

  report_stats_pre(info);

  DBUG_PRINT("backup",("Writing preamble"));

  if (write_preamble(info, s))
  {
    report_error((m_error= ER_BACKUP_UNKNOWN_ERROR));
    DBUG_RETURN(m_error);
  }

  DBUG_PRINT("backup",("Writing table data"));

  BACKUP_BREAKPOINT("backup_data");

  if ((m_error= write_table_data(m_thd, *this, info, s))) // reports errors
    DBUG_RETURN(m_error);

  DBUG_PRINT("backup",("Writing summary"));

  if (write_summary(info, s))
  {
    report_error((m_error= ER_BACKUP_UNKNOWN_ERROR));
    DBUG_RETURN(m_error);
  }

  report_stats_post(info);

  DBUG_PRINT("backup",("Backup done."));
  BACKUP_BREAKPOINT("backup_done");

  DBUG_RETURN(0);
}


/**
  Restore objects saved in backup image.

  @pre @c prepare_for_restore() method was called.
*/
int Backup_restore_ctx::do_restore()
{
  DBUG_ENTER("do_restore");

  DBUG_ASSERT(is_valid());
  DBUG_ASSERT(m_state == PREPARED_FOR_RESTORE);
  DBUG_ASSERT(m_thd);
  DBUG_ASSERT(m_stream);
  DBUG_ASSERT(m_catalog);

  using namespace backup;

  IStream &s= *static_cast<IStream*>(m_stream);
  Restore_info &info= *static_cast<Restore_info*>(m_catalog);

  report_stats_pre(info);

  DBUG_PRINT("restore",("Restoring meta-data"));

  disable_fkey_constraints();

  if (read_meta_data(info, s))
  {
    report_error((m_error= ER_BACKUP_UNKNOWN_ERROR));
    DBUG_RETURN(m_error);
  }

  s.next_chunk();

  DBUG_PRINT("restore",("Restoring table data"));

  // Here restore drivers are created to restore table data
  if ((m_error= restore_table_data(m_thd, *this, info, s))) // reports errors
    DBUG_RETURN(m_error);

  DBUG_PRINT("restore",("Done."));

  if (read_summary(info, s))
  {
    report_error((m_error= ER_BACKUP_UNKNOWN_ERROR));
    DBUG_RETURN(m_error);
  }

  report_stats_post(info);

  DBUG_RETURN(0);
}

/*************************************************

   Implementation of Backup_info class

 *************************************************/

/**
  Create @c Backup_info instance and prepare it for populating with objects.
 
  Snapshots created by the built-in backup engines are added to @c snapshots
  list to be used in the backup engine selection algoritm in 
  @c find_backup_engine().
 */
Backup_info::Backup_info(Backup_restore_ctx &ctx):
  m_state(ERROR), m_ctx(ctx), 
  native_snapshots(8)
{
  using namespace backup;

  Snapshot_info *snap;

  bzero(m_snap, sizeof(m_snap));

  // create default and CS snapshot objects - order is important!
  
  snap= new CS_snapshot(m_ctx); // reports errors

  if (!snap || !snap->is_valid())
    return;

  snapshots.push_back(snap);

  snap= new Default_snapshot(m_ctx);  // reports errors

  if (!snap || !snap->is_valid())
    return;

  snapshots.push_back(snap);
  
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
}

/**
  Select database object for backup.
  
  The object is added to the backup image's catalogue. A pointer to the
  servers obj::Obj instance is saved for later usage.
 */ 
backup::Image_info::Db* Backup_info::add_db(obs::Obj *obj)
{
  ulong pos= db_count();
  
  DBUG_ASSERT(obj);
  
  Db *db= Image_info::add_db(*obj->get_name(), pos);  // reports errors
  
  if (db)
    db->m_obj_ptr= obj;

  return db;  
}

/**
  Select given databases for restore.

  For each database, all objects stored in that database are also added to
  the image.

  @todo Report errors.
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
    
    if (is_internal_db_name(&db_name))
    {
      m_ctx.report_error(backup::log_level::ERROR, ER_BACKUP_CANNOT_INCLUDE_DB,
                         db_name.c_ptr());
      goto error;
    }
    
    obs::Obj *obj= get_database(&db_name);

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
    else
    {
      if (!unknown_dbs.is_empty())
        unknown_dbs.append(",");
      unknown_dbs.append(*obj->get_name());
      
      delete obj;
    }
  }

  if (!unknown_dbs.is_empty())
  {
    m_ctx.report_error(ER_BAD_DB_ERROR,unknown_dbs.c_ptr());
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

  @todo Report errors.
*/
int Backup_info::add_all_dbs()
{
  using namespace obs;

  int res= 0;
  ObjIterator *dbit= get_databases(m_ctx.m_thd);
  
  if (!dbit)
  {
    m_ctx.report_error(ER_BACKUP_LIST_DBS);
    return ERROR;
  }
  
  obs::Obj *obj;
  
  while ((obj= dbit->next()))
  {
    // skip internal databases
    if (is_internal_db_name(obj->get_name()))
    {
      DBUG_PRINT("backup",(" Skipping internal database %s",obj->get_name()->ptr()));
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
  Add to image all objects belonging to a given database.
 */
int Backup_info::add_db_items(Db &db)
{
  using namespace obs;

  ObjIterator *it= get_db_tables(m_ctx.m_thd,&db.name()); 

  /*
    If error debugging is switched on (see debug.h) then I_S.TABLES access
    error will be triggered when backing up database whose name starts with 'a'.
   */
  TEST_ERROR_IF(db.name().ptr()[0]=='a');

  if (!it || TEST_ERROR)
  {
    m_ctx.report_error(ER_BACKUP_LIST_DB_TABLES,db.name().ptr());
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
    Table *tbl= add_table(db,obj); // reports errors

    if (!tbl)
    {
      delete obj;
      goto error;
    }

    /*
      TODO: When we handle per-table objects, don't forget to add them here
      to the catalogue.
     */ 
  }

  goto finish;

 error:

  res= res ? res : ERROR;
  m_state= ERROR;
  
 finish:

  delete it;
  return res;
}


/// Return storage engine of a given table.
inline
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
    ::intern_close_table(table);
    my_free(table, MYF(0));
  }
  
  return se;
}

/// Determine if a given storage engine has native backup engine.
inline
static
bool has_native_backup(storage_engine_ref se)
{
  handlerton *hton= se_hton(se);

  return hton && hton->get_backup_engine;
}

/**
  Find backup engine which can backup data of a given table.

  @param[in] tbl  Table_ref describing the table

  @returns pointer to a Snapshot_info instance representing 
  snapshot to which the given table can be added. 

  @todo Add error messages.
 */
backup::Snapshot_info* 
Backup_info::find_backup_engine(const backup::Table_ref &tbl)
{
  using namespace backup;

  Table_ref::describe_buf buf;
  Snapshot_info *snap= NULL;
  
  DBUG_ENTER("Backup_info::find_backup_engine");

  // See if table has native backup engine

  storage_engine_ref se= get_storage_engine(m_ctx.m_thd, tbl);
  
  if (!se)
  {
    // TODO: better error message
    m_ctx.report_error(ER_BACKUP_TABLE_OPEN,tbl.describe(buf));
    DBUG_RETURN(NULL);
  }
  
  snap= native_snapshots[se];
  
  if (!snap)
    if (has_native_backup(se))
    {
      Native_snapshot *nsnap= new Native_snapshot(se);
      DBUG_ASSERT(nsnap);
      snapshots.push_front(nsnap);
      native_snapshots.insert(se,nsnap);

      /*
        Question: Can native snapshot for a given storage engine not accept
        a table using that engine? If yes, then what to do in that case - error 
        or try other (default) snapshots?
       */     
      DBUG_ASSERT(nsnap->accept(tbl,se));
      snap= nsnap;
    }
  
  /* 
    If we couldn't locate native snapshot for that table - iterate over
    all existing snapshots and see if one of them can accept the table.
    At the end, the snapshots using default backup engines will be tried.
  */
    
  if (!snap)
  {
    List_iterator<Snapshot_info> it(snapshots);
    
    while ((snap= it++))
      if (snap->accept(tbl,se))
        break;
  }

  if (!snap)
    m_ctx.report_error(ER_BACKUP_NO_BACKUP_DRIVER,tbl.describe(buf));
  
  DBUG_RETURN(snap);
}

namespace {

/**
  Implementation of @c Table_ref which gets table identity from a server
  object instance - to be used in @c Backup_info::add_table().
 */ 
class Tbl: public backup::Table_ref
{
 public:

   Tbl(obs::Obj *obj): backup::Table_ref(*obj->get_db_name(),*obj->get_name())
   {}

   ~Tbl()
   {}
};

} // anonymous namespace

/**
  Select table object for backup.

  The object is added to the backup image's catalogue. A pointer to the
  servers obj::Obj instance is saved for later usage. This method picks 
  the best available backup engine for the table using @c find_backup_engine() 
  method.

  @todo Correctly handle temporary tables.
  @todo Implement error debug code.
*/
backup::Image_info::Table* Backup_info::add_table(Db &dbi, obs::Obj *obj)
{
  Table *tbl= NULL;
  
  DBUG_ASSERT(obj);

  Tbl t(obj);
  // TODO: skip table if it is a tmp one
  
  /*
    TODO:
    If error debugging is switched on (see debug.h) then any table whose
    name starts with 'b' will trigger error when added to backup image.
    TEST_ERROR_IF(t.name().ptr()[0]=='b');
   */

  backup::Snapshot_info *snap= find_backup_engine(t); // reports errors

  if (!snap)
    return NULL;

  // add table to the catalogue

  ulong pos= snap->table_count();
  
  tbl= Image_info::add_table(dbi, t.name(), *snap, pos);  // reports errors
  
  if (!tbl)
    return NULL;

  tbl->m_obj_ptr= obj;

  DBUG_PRINT("backup",(" table %s backed-up with %s engine (snapshot %d)",
                      t.name().ptr(), snap->name(), snap->m_no));
  return tbl;
}


namespace backup {

/*************************************************

    Implementation of Mem_allocator class.

 *************************************************/

/// All allocated memory segments are linked into a list using this structure.
struct Mem_allocator::node
{
  node *prev;
  node *next;
};

Mem_allocator::Mem_allocator(): first(NULL)
{}

/// Deletes all allocated segments which have not been freed.
Mem_allocator::~Mem_allocator()
{
  node *n= first;

  while (n)
  {
    first= n->next;
    my_free(n,MYF(0));
    n= first;
  }
}

/**
  Allocate memory segment of given size.

  Extra memory is allocated for a @c node structure which holds pointers
  to previous and next segment in the segments list. This is used when
  deallocating allocated memory in the destructor.
*/
void* Mem_allocator::alloc(size_t howmuch)
{
  void *ptr= my_malloc(sizeof(node)+howmuch, MYF(0));

  if (!ptr)
    return NULL;

  node *n= (node*)ptr;
  ptr= n + 1;

  n->prev= NULL;
  n->next= first;
  if (first)
    first->prev= n;
  first= n;

  return ptr;
}

/**
  Explicit deallocation of previously allocated segment.

  The @c ptr should contain an address which was obtained from
  @c Mem_allocator::alloc().

  The deallocated fragment is removed from the allocated fragments list.
*/
void Mem_allocator::free(void *ptr)
{
  if (!ptr)
    return;

  node *n= ((node*)ptr) - 1;

  if (first == n)
    first= n->next;

  if (n->prev)
    n->prev->next= n->next;

  if (n->next)
    n->next->prev= n->prev;

  my_free(n,MYF(0));
}

} // backup namespace


/*************************************************

               CATALOGUE SERVICES

 *************************************************/

/**
  Memory allocator for backup stream library.

  @pre @c prepare_stream_memory() has been called (i.e., the Mem_allocator
  instance is created.
 */
extern "C"
bstream_byte* bstream_alloc(unsigned long int size)
{
  using namespace backup;

  DBUG_ASSERT(Backup_restore_ctx::mem_alloc);

  return (bstream_byte*)Backup_restore_ctx::mem_alloc->alloc(size);
}

/**
  Memory deallocator for backup stream library.
*/
extern "C"
void bstream_free(bstream_byte *ptr)
{
  using namespace backup;

  if (Backup_restore_ctx::mem_alloc)
    Backup_restore_ctx::mem_alloc->free(ptr);
}

/**
  Prepare @c Restore_info object for populating it with items read from
  backup image's catalogue.

  At this point we know the list of table data snapshots present in the image
  (it was read from image's header). Here we create @c Snapshot_info object
  for each of them.

  @todo Report errors.
*/
extern "C"
int bcat_reset(st_bstream_image_header *catalogue)
{
  using namespace backup;

  uint no;

  Restore_info *info= static_cast<Restore_info*>(catalogue);

  /*
    Iterate over the list of snapshots read from the backup image (and stored
    in snapshot[] array in the catalogue) and for each snapshot create a 
    corresponding Snapshot_info instance. A pointer to this instance is stored
    in m_snap[] array.
   */ 

  for (no=0; no < info->snap_count(); ++no)
  {
    st_bstream_snapshot_info *snap= &info->snapshot[no];

    DBUG_PRINT("restore",("Creating info for snapshot no %d",no));

    switch (snap->type) {

    case BI_NATIVE:
    {
      backup::LEX_STRING name_lex(snap->engine.name.begin, snap->engine.name.end);
      storage_engine_ref se= get_se_by_name(name_lex);
      handlerton *hton= se_hton(se);

      if (!hton)
      {
        // TODO: report error
        return BSTREAM_ERROR;
      }

      if (!hton->get_backup_engine)
      {
        // TODO: report error
        return BSTREAM_ERROR;
      }

      info->m_snap[no]= new Native_snapshot(snap->version, se);
      // TODO: handle errors during creation of the snapshot object

      break;
    }

    case BI_CS:
      info->m_snap[no]= new CS_snapshot(info->m_ctx, snap->version);
      // TODO: handle errors during creation of the snapshot object
      break;

    case BI_DEFAULT:
      info->m_snap[no]= new Default_snapshot(info->m_ctx, snap->version);
      // TODO: handle errors during creation of the snapshot object
      break;

    default:
      DBUG_PRINT("restore",("Unknown snapshot type %d",
                            info->snapshot[no].type));
      return BSTREAM_ERROR;
    }

    if (!info->m_snap[no])
    {
      // TODO: report error
      return BSTREAM_ERROR;
    }

    info->m_snap[no]->m_no= no+1;
    info->m_ctx.report_driver(info->m_snap[no]->name());
  }

  return BSTREAM_OK;
}

/**
  Called after reading backup image's catalogue and before processing
  metadata and table data.

  Nothing to do here.
*/
extern "C"
int bcat_close(st_bstream_image_header *catalogue)
{
  return BSTREAM_OK;
}

/**
  Add item to restore catalogue.

  @todo Report errors.
*/
extern "C"
int bcat_add_item(st_bstream_image_header *catalogue, struct st_bstream_item_info *item)
{
  using namespace backup;

  Restore_info *info= static_cast<Restore_info*>(catalogue);

  backup::String name_str(item->name.begin, item->name.end);

  DBUG_PRINT("restore",("Adding item %s of type %d (pos=%ld)",
                        item->name.begin,
                        item->type,
                        item->pos));

  switch (item->type) {

  case BSTREAM_IT_DB:
  {
    Image_info::Db *db= info->add_db(name_str,item->pos); // reports errors

    if (!db)
      return BSTREAM_ERROR;

    return BSTREAM_OK;
  }

  case BSTREAM_IT_TABLE:
  {
    st_bstream_table_info *it= (st_bstream_table_info*)item;

    DBUG_PRINT("restore",(" table's snapshot no is %d",it->snap_no));

    Snapshot_info *snap= info->m_snap[it->snap_no];

    if (!snap)
    {
      // TODO: report error
      return BSTREAM_ERROR;
    }

    Image_info::Db *db= info->get_db(it->base.db->base.pos); // reports errors

    if (!db)
      return BSTREAM_ERROR;

    DBUG_PRINT("restore",(" table's database is %s",db->name().ptr()));

    Image_info::Table *tbl= info->add_table(*db,name_str, *snap, item->pos); // reports error
    
    if (!tbl)
      return BSTREAM_ERROR;

    return BSTREAM_OK;
  }

  default:
    return BSTREAM_OK;

  } // switch (item->type)
}

/*****************************************************************

   Services for backup stream library related to meta-data
   manipulation.

 *****************************************************************/

/**
  Create given item using serialization data read from backup image.
 */ 
extern "C"
int bcat_create_item(st_bstream_image_header *catalogue,
                     struct st_bstream_item_info *item,
                     bstream_blob create_stmt,
                     bstream_blob other_meta_data)
{
  using namespace backup;
  using namespace obs;

  DBUG_ASSERT(catalogue);
  DBUG_ASSERT(item);

  Restore_info *info= static_cast<Restore_info*>(catalogue);

  Image_info::Obj *obj= find_obj(*info, *item); // reports errors

  /*
    TODO: Decide what to do when we come across unknown item:
    break the restore process as it is done now or continue
    with a warning?
  */

  if (!obj)
    return BSTREAM_ERROR; // find_obj should report errors

  backup::String sdata(create_stmt.begin, create_stmt.end);

  DBUG_PRINT("restore",("Creating item of type %d pos %ld: %s",
                         item->type, item->pos, sdata.ptr()));
  /*
    Note: The instance created by Image_info::Obj::materialize() is deleted
    when *info is destroyed.
   */ 
  obs::Obj *sobj= obj->materialize(0, sdata);

  if (!sobj)
  {
    // TODO: report error
    return BSTREAM_ERROR;
  }

  if (sobj->execute(::current_thd))
  {
    // TODO: report error (think about how execute() reports errors)
    return BSTREAM_ERROR;
  }
  
  return BSTREAM_OK;
}

/**
  Get serialization string for a given object.
  
  The catalogue should contain @c Image_info::Obj instance corresponding to the
  object described by @c item. This instance should contain pointer to @c obs::Obj 
  instance which can be used for getting the serialization string.
 */ 
extern "C"
int bcat_get_item_create_query(st_bstream_image_header *catalogue,
                               struct st_bstream_item_info *item,
                               bstream_blob *stmt)
{
  using namespace backup;
  using namespace obs;

  DBUG_ASSERT(catalogue);
  DBUG_ASSERT(item);
  DBUG_ASSERT(stmt);

  Backup_info *info= static_cast<Backup_info*>(catalogue);

  Image_info::Obj *obj= find_obj(*info, *item);

  if (!obj)
  {
    // TODO: warn that object was not found (?)
    return BSTREAM_ERROR;
  }

  DBUG_ASSERT(obj->m_obj_ptr);
  
  /*
    Note: Using single buffer here means that the string returned by
    this function will live only until the next call. This should be fine
    given the current ussage of the function inside the backup stream library.
    
    TODO: document this or find better solution for string storage.
   */ 
  
  ::String *buf= &(info->serialization_buf);
  buf->length(0);
  if (obj->m_obj_ptr->serialize(::current_thd, buf))
  {
    // TODO: report error
    return BSTREAM_ERROR;    
  }

  stmt->begin= (backup::byte*)buf->ptr();
  stmt->end= stmt->begin + buf->length();

  return BSTREAM_OK;
}

/**
  Get extra meta-data (if any) for a given object.
 
  @note Extra meta-data is not used currently.
 */ 
extern "C"
int bcat_get_item_create_data(st_bstream_image_header *catalogue,
                            struct st_bstream_item_info *item,
                            bstream_blob *data)
{
  /* We don't use any extra data now */
  return BSTREAM_ERROR;
}


/*************************************************

                 Helper functions

 *************************************************/

/**
  Report errors.

  Current implementation reports the last error saved in the logger if it exist.
  Otherwise it reports error given by @c error_code.
 */
int send_error(Backup_restore_ctx &log, int error_code, ...)
{
  MYSQL_ERROR *error= log.last_saved_error();

  if (error && !util::report_mysql_error(log.m_thd,error,error_code))
  {
    if (error->code)
      error_code= error->code;
  }
  else // there are no error information in the logger - report error_code
  {
    char buf[ERRMSGSIZE + 20];
    va_list args;
    va_start(args,error_code);

    my_vsnprintf(buf,sizeof(buf),ER_SAFE(error_code),args);
    my_printf_error(error_code,buf,MYF(0));

    va_end(args);
  }

  log.report_stop(my_time(0),FALSE); // FASLE = no success
  return error_code;
}


/**
  Send a summary of the backup/restore operation to the client.

  The data about the operation is taken from filled @c Archive_info
  structure. Parameter @c backup determines if this was backup or
  restore operation.
*/
int send_reply(Backup_restore_ctx &context)
{
  Protocol *protocol= context.m_thd->protocol;    // client comms
  List<Item> field_list;                // list of fields to send
  char buf[255];                        // buffer for llstr

  DBUG_ENTER("send_reply");

  /*
    Send field list.
  */
  field_list.push_back(new Item_empty_string(STRING_WITH_LEN("backup_id"))); //op_str.c_ptr(), op_str.length()));
  protocol->send_fields(&field_list, Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF);

  /*
    Send field data.
  */
  protocol->prepare_for_resend();
  llstr(context.op_id(),buf);
  protocol->store(buf, system_charset_info);
  protocol->write();

  send_eof(context.m_thd);
  DBUG_RETURN(0);
}

namespace backup {

// Flag used for testing error reporting
#ifdef DBUG_BACKUP
bool test_error_flag= FALSE;
#endif

/// Build linked @c TABLE_LIST list from a list stored in @c Table_list object.

/*
  FIXME: build list with the same order as in input
  Actually, should work fine with reversed list as long as we use the reversed
  list both in table writing and reading.
 */
TABLE_LIST *build_table_list(const Table_list &tables, thr_lock_type lock)
{
  TABLE_LIST *tl= NULL;

  for( uint tno=0; tno < tables.count() ; tno++ )
  {
    TABLE_LIST *ptr= (TABLE_LIST*)my_malloc(sizeof(TABLE_LIST), MYF(MY_WME));
    DBUG_ASSERT(ptr);  // FIXME: report error instead
    bzero(ptr,sizeof(TABLE_LIST));

    Table_ref tbl= tables[tno];

    ptr->alias= ptr->table_name= const_cast<char*>(tbl.name().ptr());
    ptr->db= const_cast<char*>(tbl.db().name().ptr());
    ptr->lock_type= lock;

    // and add it to the list

    ptr->next_global= ptr->next_local=
      ptr->next_name_resolution_table= tl;
    tl= ptr;
    tl->table= ptr->table;
  }

  return tl;
}

} // backup namespace
