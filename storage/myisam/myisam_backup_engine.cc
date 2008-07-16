/* Copyright (C) 2007 MySQL AB

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

/**
  @file
  Online backup engine for the MyISAM storage engine.

  @see myisam_backup
*/

#define MYSQL_SERVER 1 // need it to have mysql_tmpdir defined
#include "mysql_priv.h"
#include "ha_myisam.h"
#include "myisamdef.h" // to access dfile and kfile
#include "backup/backup_engine.h"
#include "backup/backup_aux.h"         // for build_table_list()
#include <hash.h>

/**
  Online backup engine for the MyISAM storage engine.

  Reference of the Online Backup API:
  http://forge.mysql.com/source/OnlineBackup.

  Here is how the MyISAM online backup works.
  It is online because we dirtily copy the data and index files,
  and the tables maintain a physical idempotent log of changes done to them
  during the copy process, applying this log to the dirty copy yields a clean
  table corresponding to how the original table was when logging ended.
  Idempotent means that if you apply such log to a table, then applying it a
  second time has no effect.

  A condition for this to work is that any update done to a table after the
  copy process started must be present in the log. See the comment of
  mi_log_start_physical() for how this is ensured.

  HOW THE BACKUP WORKS

  In Backup::begin(), we instruct all needed tables to do backup
  logging; this does not have to wait for existing updates to complete,
  neither does it stall new updates.

  Then we dirtily copy them in Backup::get_data(). That copy is intensive on
  the hard drive, so can be optionally throttled (via a configurable sleep).

  When the copy process is done with tables, it signals the backup kernel
  that it is ready to lock tables (to create a validity point).
  To not waste its time until the Backup::prelock() request is sent by the
  backup kernel, the copy process starts copying the log.

  Now the Backup::prelock() request comes.
  To finish the backup, we need to synchronize (=read-lock) all tables of the
  backup (thus creating a consistent state accross them), stop logging for
  all of them, and unlock tables. This lock can wait for a long time if there
  is a long running update. If it waited a long time, other drivers which have
  already executed their lock(), would stay locked for a long time. To avoid
  that, we do all the locking work before Backup::lock(), in
  Backup::prelock() (called before lock() on any driver). Backup::prelock()
  itself is not allowed to block, because it is called from the backup
  kernel's thread: so it launches a separate thread (which will issue a LOCK
  TABLES READ on our tables) and does not wait for completion of LOCK TABLES
  READ: it immediately returns backup::OK which means "I have not completed my
  preparations for locking".

  In Backup::get_data(), the driver monitors the status of the locking
  thread, and when finally that thread has managed to get its locks, we stop
  logging and reply backup::READY.

  So note the difference: this time, we have to wait for all updates to
  finish, and stall new ones.

  Next Backup::get_data() calls, if there are, send the final tail of the
  log.

  Backup::lock() comes, it's an empty operation for the driver.

  Later we get a Backup::unlock() request. That kills the locking thread,
  which thus unlocks tables. And Backup::end() cleans up memory.

  HOW THE RESTORE WORKS

  In Restore::send_data() we receive data which we write to tables (those
  tables have just been created with their correct structure, but no data, by
  the backup kernel). We similarly restore the log.

  In Restore::end(), we apply the log to tables, making them clean.
  If of the index file we backed up only the header (an option), we here do
  an index rebuild.
  Voila, the table is ready to work.

  @todo if an index rebuild is needed, possibly do it at backup time.
*/
namespace myisam_backup {

using backup::byte;
using backup::result_t;
using backup::version_t;
using backup::Table_list;
using backup::Table_ref;
using backup::Buffer;

/**
  The current version of the format stored in MyISAM backup images by this
  code. Increase it when making a backward-incompatible change.
*/
#define MYISAM_BACKUP_VERSION 1

/** Like Table_ref but with file name added */
class Myisam_table_ref
{
public:
  Myisam_table_ref(const Table_ref &);
protected:
  String db, name;
  String file_name; ///< concatenation of db and table name
};


Myisam_table_ref::Myisam_table_ref(const Table_ref &tbl)
{
  int error= 0;
  char path[FN_REFLEN];

  /**
    We keep local copies of the db and name. This is because during restore,
    the Table_ref is apparently modified before the Table_restore is done
    (symptom is that starting from second Table_restore::send_data() we see
    Table_ref being garbage, and this is a problem in
    Table_restore::post_restore()). Rafal suspects a bug.
    @todo Once fixed, we can replace "String db,name" by "Table_ref &ref",
    and this will save memory.
    As Rafal is changing relevant code now, it may go away.
  */
  if (db.append(tbl.db().name()))
    error= 1;
  if (name.append(tbl.name()))
    error= 1;
  /*
    Note: when we repair the table, we use open_temporary_table() which
    requires db and table name separated. The internal_name is the
    translated table name with ASCII characters only.
  */
  (void) tbl.internal_name(path, sizeof(path));
  if (file_name.append(path))
    error= 1;
  /*
    If one of the string allocations failed, clear all. This should be
    noticed later, when we try to use the information.
  */
  if (error)
  {
    db.set("", 0, system_charset_info);
    name.set("", 0, system_charset_info);
    file_name.set("", 0, system_charset_info);
  }
}


/**
  Backup engine class. It is the master class: a Backup_engine creates a
  Backup_driver and a corresponding Restore_driver. @see backup::Engine.
*/
class Engine: public Backup_engine
{
  public:
    Engine() {}
    virtual const version_t version() const { return MYISAM_BACKUP_VERSION; };
    virtual result_t get_backup(const uint32, const Table_list &,
                                Backup_driver* &);
    virtual result_t get_restore(const version_t, const uint32,
                                 const Table_list &,Restore_driver* &);
  virtual void free() { delete this; }
};

/*************************
 *
 *  BACKUP FUNCTIONALITY
 *
 *************************/

class Object_backup;


/**
  Handles backup orders received from the backup kernel (implements the API).
*/
class Backup: public Backup_driver
{
public:
  Backup(const Table_list &);
  virtual ~Backup();
  /** Estimates total size of backup. @todo improve it */
  virtual size_t    size() { return UNKNOWN_SIZE; };
  /** Estimates size of backup before lock. @todo improve it */
  virtual size_t    init_size() { return UNKNOWN_SIZE; };
  virtual result_t  begin(const size_t);
  virtual result_t  end();
  virtual result_t  get_data(Buffer &);
  virtual result_t  prelock();
  virtual result_t  lock();
  virtual result_t  unlock();
  virtual result_t  cancel()
    {
      return backup::OK ; // free() will be called and suffice
    };
  virtual void free() { delete this; };
  void lock_tables_TL_READ_NO_INSERT();

private:
  enum { DUMPING_DATA_INDEX_FILES,
         DUMPING_LOG_FILE_BEFORE_TABLES_ARE_LOCKED,
         DUMPING_LOG_FILE_AFTER_TABLES_ARE_LOCKED,
         DONE, ERROR } state;
  Object_backup  *image; ///< object in backup currently
  uint stream; ///< which stream we are currently writing to
  char backup_log_name[FN_REFLEN];
  /**
    All db||table names in a HASH structure. Passed to MyISAM functions for
    them to detect if a table is part of the backup (=> should do logging) or
    not.
  */
  HASH *hash_of_tables;
  /**
     Locking of tables goes through these states. It is a delicate variable
     which must be set correctly after inspecting thread-safety and race
     conditions.
  */
  enum { LOCK_NOT_STARTED, LOCK_STARTED, LOCK_ACQUIRED, LOCK_ERROR }
    lock_state;
  /**
    The locking thread (so that we can kill it). Creating a validity point is
    only possible by locking all tables (it is the only way to have tables
    consistent with each other, as we have no UNDO log). But locking via
    thr_lock() is blocking. So, to have a non-blocking prelock() call, this
    locking is done in a separate thread (named "the locking thread").
  */
  THD *lock_thd;
  bool cannot_delete_lock_thd;
  pthread_cond_t COND_lock_state; ///< for communication with locking thread
  void kill_locking_thread();
  static const size_t bytes_between_sleeps= 10*1024*1024;
  /** After copying bytes_between_sleeps we sleep sleep_time */
  ulong sleep_time;
  size_t bytes_since_last_sleep; ///< how many bytes sent since we last slept
};

/**
  When we send a backup packet to the backup kernel, we prefix it with a code
  which tells which type of file this packet belongs to. Starts at 1 because
  garbage is often zeros and we want to spot it.
*/
enum enum_file_code { DATA_FILE_CODE= 1,
                      WHOLE_INDEX_FILE_CODE, HEADER_INDEX_FILE_CODE,
                      LOG_FILE_CODE };

/** An object to backup; in practice, a table or the log */
class Object_backup
{
public:
  virtual result_t get_data(Buffer &)= 0;
  virtual ~Object_backup() {};
  bool internal_error() { return state == ERROR; }
  /**
    The only reason to have an end() and call it from the destructor, instead
    of putting the code into the destructor, is that when the caller does a
    "delete image", it cannot be told about errors, while if the caller does
    "image->end()" (and then "delete image") it can see an error.
  */
  virtual result_t end()= 0; ///< cleanups
protected:
  enum { OK, ERROR } state; ///< serves to detect an error during construction
};


/**
  An object to back up is made of one or more such files. This class does not
  open the file, user has to open it. This class provides a helper method if
  its user wants to close the file.
*/
class File_backup
{
public:
  File_backup() : fd(-1), backup_file_size(0) {}

  /**
    Initializes the object.

    @param  fd_arg        file descriptor to attach to
    @param  file_size_arg copy should stop after copying that many bytes
    @param  file_code_arg code to store at start of each sent data packet
  */

  void init(int fd_arg, my_off_t file_size_arg, enum_file_code file_code_arg)
    { fd= fd_arg; file_size= file_size_arg; file_code= file_code_arg; }

  result_t get_data(Buffer &);
  result_t close_file();
private:
  int fd; ///< file descriptor
  /**
    After backing up that many bytes of the file, we can stop. In case of
    ftruncate() happening to the file, we may even copy less than this size.
  */
  my_off_t file_size;
  enum_file_code file_code; ///< code stored at start of each backup block
  my_off_t backup_file_size; ///< how much of the file we already backed up
};


/** Handles backing up a single table */
class Table_backup: public Myisam_table_ref, public Object_backup
{
public:
  Table_backup(const backup::Table_ref &);
  virtual ~Table_backup();
  virtual result_t get_data(Buffer &);
  virtual result_t end(); ///< cleanups
private:
  File_backup dfile_backup, kfile_backup;
  enum { DATA_FILE, INDEX_FILE } in_file; ///< which file we are dumping now
};


/** Handles backing up the log */
class Log_backup: public Object_backup
{
public:
  Log_backup(const char *);
  virtual ~Log_backup();
  virtual result_t get_data(Buffer &);
  virtual result_t end();
private:
  const char *log_name;
  File_backup log_file_backup;
  bool log_deleted; ///< if we have already deleted the log or not
};


/**
  Creates a backup driver, per the backup API. @see backup::Engine.

  @return Operation status
    @retval backup::OK
    @retval backup::ERROR
*/

result_t Engine::get_backup(const uint32, const Table_list &tables,
                            Backup_driver* &drv)
{
  Backup *ptr= new Backup(tables);
  if (unlikely(!ptr))
    return backup::ERROR;
  drv= ptr;
  return backup::OK;
}


Backup::Backup(const Table_list &tables):
  Backup_driver(tables), state(ERROR), image(NULL), stream(1),
  hash_of_tables(NULL), lock_state(LOCK_NOT_STARTED), lock_thd(NULL),
  cannot_delete_lock_thd(FALSE), bytes_since_last_sleep(0)
{
  /*
    Driver is not ready at this point, so state is ERROR.
    This constructor cannot fail, otherwise begin() would have to detect it.
  */
  pthread_cond_init(&COND_lock_state, NULL);
}


/** Kills the locking thread when it is time to unlock tables */

void Backup::kill_locking_thread()
{
  DBUG_ENTER("myisam_backup::Backup::kill_locking_thread");
  /*
    If everything worked well, when unlock() calls us we kill the thread and
    so when free() calls us the locking thread is already dead here
    (LOCK_ERROR).
  */
retry:
  pthread_mutex_lock(&THR_LOCK_myisam);
  /* If thread started and not already dead, kill it */
  if ((lock_state != LOCK_NOT_STARTED) & (lock_state != LOCK_ERROR))
  {
    /*
      If the locking thread has not yet created THD (very unlikely), wait
      for it.
    */
    if (unlikely(lock_thd == NULL))
    {
      pthread_mutex_unlock(&THR_LOCK_myisam);
      DBUG_PRINT("info",("lock_thd not yet set"));
      sleep(1);
      goto retry;
    }
    /*
      Locking thread had time to create its THD, may be inside table locking
      (waiting for others to release locks etc), wake it up and kill it. Or it
      may have locked tables successfully, and be waiting for us to kill it.
      To do that we will use lock_thd, but how to be sure that lock_thd is not
      being deleted now? One way would be to hold THR_LOCK_myisam but
      THD::awake() can't bear it (same mutex locked twice).
      Another way is to take lock_thd->LOCK_delete (THD::awake() requires it
      anyway), but again that requires that lock_thd is not deleted while we
      access the mutex. We cannot hold THR_LOCK_myisam to get LOCK_delete,
      because that could deadlock if a some other thread is doing a KILL on
      the locking thread (it would indeed take LOCK_delete and then
      THR_LOCK_myisam to wake up the locking thread).
      So So we set a flag:
    */
    cannot_delete_lock_thd= TRUE;
    pthread_mutex_unlock(&THR_LOCK_myisam);
    /*
      So now lock_thd cannot be destroyed.
      We kill the thread (which will in particular work if it is waiting for
      some table locks).
    */
    pthread_mutex_lock(&lock_thd->LOCK_delete);
    lock_thd->awake(THD::KILL_CONNECTION);
    pthread_mutex_unlock(&lock_thd->LOCK_delete);
    /* won't look at lock_thd anymore, allow its deletion */
    pthread_mutex_lock(&THR_LOCK_myisam);
    cannot_delete_lock_thd= FALSE;
    /* we wake up thread if it was blocked on the bool above */
    pthread_cond_broadcast(&COND_lock_state);
    /* And we wait for the thread to inform of its death */
    while (lock_state != LOCK_ERROR)
      pthread_cond_wait(&COND_lock_state, &THR_LOCK_myisam);
  }
  pthread_mutex_unlock(&THR_LOCK_myisam);
  DBUG_VOID_RETURN;
}


/**
  This destructor is only called by the class' free().
  It cleans up any leftover the driver could have. It is safe to call it at
  any point. In a normal (no error) situation, the hash freeing is the only
  operation done here, all the rest should already have been done by earlier
  stages.
*/

Backup::~Backup()
{
  DBUG_ENTER("myisam_backup::Backup::~Backup");
  /* If we had already started backup logging, we must dirtily stop it */
  mi_log(MI_LOG_ACTION_CLOSE_INCONSISTENT, MI_LOG_PHYSICAL, NULL, NULL);
  delete image;
  if (hash_of_tables)
  {
    hash_free(hash_of_tables);
    delete hash_of_tables;
    hash_of_tables= NULL;
  }
  kill_locking_thread();
  pthread_cond_destroy(&COND_lock_state);
  DBUG_VOID_RETURN;
}


/** Usual parameter to hash_init() */

static uchar
*backup_get_table_from_hash_key(const uchar *lsc, size_t *length,
                                my_bool not_used __attribute__ ((unused)))
{
  const ::LEX_STRING *ls= reinterpret_cast<const ::LEX_STRING *>(lsc);
  *length= ls->length;
  return reinterpret_cast< uchar *>(ls->str);
}


/** Usual parameter to hash_init() */

static void backup_free_hash_key(void *lsv)
{
  my_free(lsv, MYF(MY_WME));
}


#define SET_STATE_TO_ERROR_AND_DBUG_RETURN {                                 \
    state= ERROR;                                                       \
    DBUG_PRINT("error",("driver got an error at %s:%d",__FILE__,__LINE__)); \
    DBUG_RETURN(backup::ERROR); }

/* use this one only in constructors */
#define SET_STATE_TO_ERROR_AND_DBUG_VOID_RETURN {                       \
    state= ERROR;                                                       \
    DBUG_PRINT("error",("driver got an error at %s:%d",__FILE__,__LINE__)); \
    DBUG_VOID_RETURN; }


/**
  Sets MyISAM in a state ready for the copy to start. I.e. builds
  a hash of tables and starts MyISAM physical logging for those tables.

  @return Operation status
    @retval backup::OK
    @retval backup::ERROR
*/

result_t Backup::begin(const size_t)
{
  DBUG_ENTER("myisam_backup::Backup::begin");
  DBUG_PRINT("info",("%lu tables", m_tables.count()));

  /*
    per the API, all significant allocations (large mem, opening files) must
    not be in the constructor but in begin() or later.
  */
  DBUG_ASSERT(!hash_of_tables); // no double begin() call or reuse of driver
  DBUG_ASSERT(m_tables.count() > 0); // or bug in the backup kernel
  /*
    If external locking is on, some other processes may modify our tables
    while we are copying them, those modifications will not reach the log,
    backup will be corrupted.
  */
  if (!my_disable_locking || !myisam_single_user)
  {
    my_error(ER_GET_ERRMSG, MYF(0),
             MYISAM_ERR_NO_BACKUP_WITH_EXTERNAL_LOCKING,
             MYISAM_ERR(MYISAM_ERR_NO_BACKUP_WITH_EXTERNAL_LOCKING), "MyISAM");
    SET_STATE_TO_ERROR_AND_DBUG_RETURN;
  }
  hash_of_tables= new HASH;
  if (!hash_of_tables ||
      hash_init(hash_of_tables, &my_charset_bin, m_tables.count(), 0, 0,
                (hash_get_key)backup_get_table_from_hash_key,
                (hash_free_key)backup_free_hash_key, 0))
    SET_STATE_TO_ERROR_AND_DBUG_RETURN;
  /* Build the hash of tables for the MyISAM layer (mi_backup_log.c etc) */
  for (uint n=0 ; n < m_tables.count() ; n++ )
  {
    char path[FN_REFLEN];
    char unique_file_name[FN_REFLEN], *str;
    size_t str_len;
    ::LEX_STRING *hash_key;

    /*
      The internal_name is the translated table name with ASCII
      characters only.
    */
    (void) m_tables[n].internal_name(path, sizeof(path));
    if (my_realpath(unique_file_name,
                    fn_format(unique_file_name, path, "", MI_NAME_IEXT,
                              MY_UNPACK_FILENAME), MYF(MY_WME)))
        SET_STATE_TO_ERROR_AND_DBUG_RETURN;
    str_len= strlen(unique_file_name);
    my_multi_malloc(MYF(MY_WME),
                    &hash_key, sizeof(*hash_key),
                    &str, static_cast<uint>(str_len), NullS);
    if (!hash_key)
      SET_STATE_TO_ERROR_AND_DBUG_RETURN;
    memcpy(str, unique_file_name, str_len);
    hash_key->length= str_len;
    hash_key->str= str;
    if (my_hash_insert(hash_of_tables,
                       reinterpret_cast< uchar *>(hash_key)))
    {
      my_free(hash_key, MYF(MY_WME));
      SET_STATE_TO_ERROR_AND_DBUG_RETURN;
    }
    DBUG_PRINT("info",("table '%.*s' inserted in hash",
                       static_cast<int>(hash_key->length), hash_key->str));
  }

  {
    THD *thd= current_thd;
    /*
      If tmpdir is in RAM (/dev/shm etc), we may exhaust it if our log is big
    */
    my_snprintf(backup_log_name, sizeof(backup_log_name),
                "%s/%s%lx_%lx_%x-backuplog", mysql_tmpdir,
                tmp_file_prefix, current_pid, thd->thread_id,
                thd->tmp_table++); // it's not a tmp table but what...
    unpack_filename(backup_log_name, backup_log_name);
  }

  {
    /**
      Until there exists a framework by which the user tells, via SQL,
      indications on how it wants the backup, and by which the backup kernel
      tells it to the driver (API), we resort to this.
    */
    char *env_arg= getenv("MYISAM_BACKUP_NO_INDEX");
    /* By default we log index pages */
    mi_log_index_pages_physical= !(env_arg && atoi(env_arg));
    env_arg= getenv("MYISAM_BACKUP_SLEEP");
    /*
      By default we don't sleep at all; however, 500 ms every 10MB gives a
      low penalty on clients, so it can be a good choice.
    */
    sleep_time= env_arg ? atoi(env_arg) : 0;
  }

  if (mi_log(MI_LOG_ACTION_OPEN, MI_LOG_PHYSICAL,
             backup_log_name, hash_of_tables))
    SET_STATE_TO_ERROR_AND_DBUG_RETURN;

  state= DUMPING_DATA_INDEX_FILES;
  DBUG_RETURN(backup::OK);
}


/**
  If some error happened, end() is not called but free() is. So we do all
  cleanup in free() i.e. in the destructor, and nothing here.

  @return Operation status
    @retval backup::OK
*/

result_t Backup::end()
{
  DBUG_ENTER("myisam_backup::Backup::end");
  DBUG_RETURN(backup::OK);
}


/**
  Sends backup data for tables and log to the backup kernel.

  @param  buf             reference to Buffer where data should be put

  @return Operation status (see the API for when they are returned)
    @retval backup::OK
    @retval backup::DONE
    @retval backup::READY
    @retval backup::ERROR
*/

result_t Backup::get_data(Buffer &buf)
{
  result_t ret;
  DBUG_ENTER("myisam::backup::Backup::get_data");
  DBUG_PRINT("enter",("stream %d",stream));

  /* we are currently on stream 'stream' */
  buf.table_num= stream;

  /*
    Rafal and I agreed that one single ERROR from the driver will cause the
    upper layer to not call the driver anymore except for free().
  */
  DBUG_ASSERT(state != ERROR);
  DBUG_ASSERT(buf.data != NULL); // to check that caller gave room

  if (state == DONE)
  {
    /*
      We never come here, because after returning from the call where we sent
      the last piece of the last stream (when we set our internal state to
      DONE), all streams were closed, so the upper layer wouldn't call us
      again. At least it was so during testing. But if it calls us, we do all
      that the API expects us to do:
    */
    buf.size= buf.table_num= 0;
    buf.last= TRUE;
    DBUG_RETURN(backup::DONE);
  }

  if (unlikely(image == NULL))
  {
    /*
      Let's create it.
      Table 0 will be image 1 on stream 1. Table N will be image N+1 on stream
      N+1. Log will be image 0 on stream 0.
    */
    if (stream >= 1)
      image= new Table_backup(m_tables[stream-1]);
    else
      image= new Log_backup(backup_log_name);
    if (image == NULL || image->internal_error())
      SET_STATE_TO_ERROR_AND_DBUG_RETURN;
  }

  if ((ret= image->get_data(buf)) != backup::OK)
    SET_STATE_TO_ERROR_AND_DBUG_RETURN;

  if (sleep_time)
  {
    bytes_since_last_sleep+= buf.size;
    /* sched_yield() is not as flexible (higher penalty) as sleep() */
    if (bytes_since_last_sleep > bytes_between_sleeps)
    {
      my_sleep(sleep_time * 1000UL);
      bytes_since_last_sleep= 0;
    }
  }

  if (state == DUMPING_LOG_FILE_BEFORE_TABLES_ARE_LOCKED)
  {
    DBUG_ASSERT(stream == 0);
    /*
      We are sending the log; even if reached its EOF, some more may be
      appended to it before prelock() ends, so this is not the stream's end.
    */
    buf.last= FALSE;
    /*
      API docs say we should return READY, but Rafal says OK is better (one
      READY to signal end of initial phase; then OKs; one READY to signal end
      of prelock(); then OKs).
    */
    if (lock_state == LOCK_NOT_STARTED)
      DBUG_RETURN(backup::OK);
    /* Let's see if the locking thread has finished locking all tables */
    pthread_mutex_lock(&THR_LOCK_myisam);
    if (lock_state == LOCK_STARTED) // not yet
    {
      pthread_mutex_unlock(&THR_LOCK_myisam);
      DBUG_RETURN(backup::OK);
    }
    if (lock_state !=  LOCK_ACQUIRED) // it failed, so do we
    {
      pthread_mutex_unlock(&THR_LOCK_myisam);
      SET_STATE_TO_ERROR_AND_DBUG_RETURN;
    }
    DBUG_PRINT("info",("locking thread acquired locks on tables"));

    pthread_mutex_unlock(&THR_LOCK_myisam);
    if (mi_log(MI_LOG_ACTION_CLOSE_CONSISTENT, MI_LOG_PHYSICAL, NULL, NULL))
      SET_STATE_TO_ERROR_AND_DBUG_RETURN;
    state= DUMPING_LOG_FILE_AFTER_TABLES_ARE_LOCKED;
    DEBUG_SYNC(current_thd, "myisam_locking_thread_added");
    /* signal "end of prepare-for-lock, ready for lock()" */
    DBUG_RETURN(backup::READY);
  }
  else if (buf.last)
  {
    /*
      we are sending the last chunk of the image, next call will be about the
      next image:
    */
    if (image->end() != backup::OK)
      SET_STATE_TO_ERROR_AND_DBUG_RETURN;
    delete image;
    image= NULL; /* next call of this function should open the next object */
    stream++;    /* and send it on the next stream */
    if (state == DUMPING_DATA_INDEX_FILES && stream > m_tables.count())
    {
      /* all tables done */
      stream= 0; // send the log on stream 0
      state= DUMPING_LOG_FILE_BEFORE_TABLES_ARE_LOCKED;
      ret= backup::READY; // end of initial phase
    }
    else if (state == DUMPING_LOG_FILE_AFTER_TABLES_ARE_LOCKED) // log done
      state= DONE;
  }

  DBUG_RETURN(ret);
}


/**
  Creates a validity point by locking all tables. This is the only job of the
  locking thread: call this function which locks tables, then wait for being
  killed (which will unlock tables).

  @todo GUILHEM_TODO use sql/backup/be_thread.cc instead.

  @todo use a method which does not open closed tables. This will be needed
  when backing up lots of tables (more than the limit of open file
  descriptors).
*/

void Backup::lock_tables_TL_READ_NO_INSERT()
{
  THD *thd;
  TABLE_LIST *tables_in_TABLE_LIST_form=NULL ; ///< for open_and_lock_tables()
  const char thread_name[]= "MyISAM driver locking thread";
  DBUG_ENTER("myisam::backup::Backup::lock_tables_TL_READ_NO_INSERT");

  thd= new THD;
  if (unlikely(!thd))
    goto end2;
  thd->thread_stack = reinterpret_cast< char *>(&thd);
  pthread_mutex_lock(&LOCK_thread_count);
  thd->thread_id= thread_id++;
  pthread_mutex_unlock(&LOCK_thread_count);
  if (unlikely(thd->store_globals())) // for a proper MEM_ROOT
    goto end2;
  thd->init_for_queries(); // opening tables needs a proper LEX
  thd->command= COM_DAEMON;
  thd->system_thread= SYSTEM_THREAD_BACKUP;
  thd->version= refresh_version;
  thd->set_time();
  thd->main_security_ctx.host_or_ip= "";
  thd->client_capabilities= 0;
  my_net_init(&thd->net, 0);
  thd->main_security_ctx.master_access= ~0;
  thd->main_security_ctx.priv_user= 0;
  thd->real_id= pthread_self();
  /*
    Making this thread visible to SHOW PROCESSLIST is useful for
    troubleshooting a backup job (why does it stall etc).
  */
  pthread_mutex_lock(&LOCK_thread_count);
  threads.append(thd);
  pthread_mutex_unlock(&LOCK_thread_count);
  /*
    Set info for the process list. Used in test cases.
  */
  thd->query= (char*) thread_name;
  thd->query_length= sizeof(thread_name) - 1;

  lex_start(thd);
  mysql_reset_thd_for_next_command(thd);
  /*
    As locking tables can be a long operation, we need to support
    cancellability during that time. So we publish our THD now to the thread
    which created us (the "master" thread), so that it can kill us early if
    needed.
  */
  pthread_mutex_lock(&THR_LOCK_myisam);
  lock_thd= thd;
  pthread_mutex_unlock(&THR_LOCK_myisam);
  /*
    We need TL_READ_NO_INSERT (and not TL_READ) because we want to prevent
    concurrent inserts (we indeed need to freeze the tables to correspond to
    a position in the binlog).
  */
  tables_in_TABLE_LIST_form=
    backup::build_table_list(m_tables, TL_READ_NO_INSERT);
  if (!tables_in_TABLE_LIST_form)
    goto end2;
  if (open_and_lock_tables(thd, tables_in_TABLE_LIST_form))
    goto end;

  DBUG_PRINT("info",("MyISAM backup locking thread got locks"));
  pthread_mutex_lock(&THR_LOCK_myisam);
  thd->enter_cond(&COND_lock_state, &THR_LOCK_myisam,
                  "MyISAM backup: holding table locks");
  /* show master thread that we got locks */
  lock_state= LOCK_ACQUIRED;
  /* and wait for it to kill us */
  while (!thd->killed)
    pthread_cond_wait(&COND_lock_state, &THR_LOCK_myisam);
  thd->exit_cond("MyISAM backup: terminating");

end:
  DBUG_PRINT("info",("MyISAM backup locking thread dying"));
  close_thread_tables(thd);
end2:
  pthread_mutex_lock(&THR_LOCK_myisam);
  while (cannot_delete_lock_thd)
  {
    /* master thread is looking at our THD; wait for authorization */
    pthread_cond_wait(&COND_lock_state, &THR_LOCK_myisam);
  }
  lock_state= LOCK_ERROR;
  pthread_cond_broadcast(&COND_lock_state);
  pthread_mutex_unlock(&THR_LOCK_myisam);
  backup::free_table_list(tables_in_TABLE_LIST_form);
  net_end(&thd->net);
  delete thd;
  DBUG_VOID_RETURN;
}


/** Entry point for the locking thread */

pthread_handler_t separate_thread_for_locking(void *arg)
{
  my_thread_init();
  DBUG_PRINT("info", ("myisam_backup::separate_thread_for_locking"));
  pthread_detach_this_thread();
  (static_cast<Backup *>(arg))->lock_tables_TL_READ_NO_INSERT();
  my_thread_end();
  pthread_exit(0);
  return 0;
}


/**
  Launches a separate thread ("locking thread") which will lock
  tables. Locking in a separate thread is needed to have a non-blocking
  prelock() (given that thr_lock() is blocking). prelock() is indeed not
  allowed to block, or it would block the entire backup kernel (see "HOW THE
  BACKUP WORKS" at the start of this file).

  @return Operation status
    @retval backup::OK
    @retval backup::ERROR
*/

result_t Backup::prelock()
{
  DBUG_ENTER("myisam_backup::Backup::prelock");
  /* we are going to launch a thread, we need to remember to kill it */
  lock_state= LOCK_STARTED;
  {
    pthread_t th;
    if (pthread_create(&th, &connection_attrib,
                       separate_thread_for_locking, this))
    {
      lock_state= LOCK_ERROR;
      SET_STATE_TO_ERROR_AND_DBUG_RETURN;
    }
  }
  DBUG_RETURN(backup::OK);
}


result_t Backup::lock()
{
  DBUG_ENTER("myisam_backup::Backup::lock");
  /* locking was done in prelock() already, nothing to do */
  DBUG_RETURN(backup::OK);
}


result_t Backup::unlock()
{
  DBUG_ENTER("myisam_backup::Backup::unlock");
  /* kill the locking thread which owns table locks, it will unlock them */
  kill_locking_thread();
  DBUG_RETURN(backup::OK);
}


/**
  Backs up the log.

  @todo For now we read the log file from disk. We could instead try to
  "steal" it from its IO_CACHE; that might reduce the log portion which goes
  to disk, if the backup thread is fast enough to catch up on client threads
  filling the log.
*/

Log_backup::Log_backup(const char *log_name_arg) : log_name(log_name_arg),
                                                   log_deleted(FALSE)
{
  DBUG_ENTER("myisam_backup::Log_backup::Log_backup");
  int fd= my_open(log_name, O_RDONLY, MYF(MY_WME));
  if (fd < 0)
    SET_STATE_TO_ERROR_AND_DBUG_VOID_RETURN;
  /*
    Log is alone on the shared stream for now, so LOG_FILE_CODE is useless,
    except that it allows us to verify that what restore sends us is really a
    log.
  */
  log_file_backup.init(fd, ~(ULL(0)), LOG_FILE_CODE);
  state= OK;
  DBUG_VOID_RETURN;
}


/**
  Closes and deletes the log.

  @return Operation status
    @retval backup::OK
    @retval backup::ERROR
*/

result_t Log_backup::end()
{
  DBUG_ENTER("myisam_backup::Log_backup::end");
  /*
    Log is safe in the stream, or backup is cancelled, so we don't need it
    anymore.
  */
  if (log_file_backup.close_file() != backup::OK ||
      (!log_deleted && my_delete(log_name, MYF(MY_WME))))
    SET_STATE_TO_ERROR_AND_DBUG_RETURN;
  log_deleted= TRUE;
  DBUG_RETURN(backup::OK);
}


Log_backup::~Log_backup()
{
  /*
    If all went well, we don't do anything here.
    All possible failures in end() below use MYF_WME so my_error() will be
    called.
  */
  end();
}


/** The header of a MYI index file always fits in this size */
#define MAX_INDEX_HEADER_SIZE (64*1024)


/**
  Opens a MyISAM table for backing it up.

  @param  tbl             The table to open
*/

Table_backup::Table_backup(const backup::Table_ref &tbl) :
  Myisam_table_ref(tbl)
{
  MI_INFO *mi_info;
  File dfiledes= -1, kfiledes= -1;
  my_off_t file_size;
  DBUG_ENTER("myisam_backup::Table_backup::Table_backup");
  DBUG_PRINT("info",("Initializing backup image for table %s",
                     file_name.ptr()));
  /*
    Here we use low-level mi_* functions as all we want is a pair of file
    descriptors.
    O_RDONLY is not ok, as it forces all instances of the table to be
    read-only (sets HA_OPTION_READ_ONLY_DATA of share->options).
    We don't use HA_OPEN_FOR_REPAIR so will fail to back up a known corrupted
    table (would be a corrupted backup).
  */
  mi_info= mi_open(file_name.ptr(), O_RDWR, 0);
  if (!mi_info) // table does not exist or is corrupted? backup not ok
    goto err;
  /*
    we create our own descriptors, to use my_read() (faster than my_pread()
    which may use mutex).
  */
  dfiledes= my_open(mi_info->s->data_file_name, O_RDONLY, MYF(MY_WME));
  kfiledes= my_open(mi_info->s->unique_file_name, O_RDONLY, MYF(MY_WME));
  if ((dfiledes < 0) || (kfiledes < 0))
    goto err;
  mi_close(mi_info);
  mi_info= NULL;
  file_size= my_seek(dfiledes, 0, SEEK_END, MYF(MY_WME));
  if (file_size == MY_FILEPOS_ERROR ||
      my_seek(dfiledes, 0, SEEK_SET, MYF(MY_WME)) == MY_FILEPOS_ERROR)
    goto err;
  dfile_backup.init(dfiledes, file_size, DATA_FILE_CODE);
  if (mi_log_index_pages_physical)
  {
    file_size= my_seek(kfiledes, 0, SEEK_END, MYF(MY_WME));
    if (file_size == MY_FILEPOS_ERROR ||
        my_seek(kfiledes, 0, SEEK_SET, MYF(MY_WME)) == MY_FILEPOS_ERROR)
      goto err;
    kfile_backup.init(kfiledes, file_size, WHOLE_INDEX_FILE_CODE);
  }
  else
    kfile_backup.init(kfiledes,
                      MAX_INDEX_HEADER_SIZE /* upper limit */ ,
                      HEADER_INDEX_FILE_CODE);
  in_file= DATA_FILE; // dump the data file first (no specific reason)
  state= OK;
  DBUG_VOID_RETURN;
  /*
    Note: we are copying an index file of a table, which may have instances in
    the MySQL table cache, so after restore it will show up as
    "warning: 1 client is using or hasn't closed the table properly".
    Maybe do a quick index update on the table at the end of restore to
    remove this warning. But how to know if the problem pre-dates backup ?
  */
err:
  if (dfiledes > 0)
    my_close(dfiledes, MYF(MY_WME));
  if (kfiledes > 0)
    my_close(kfiledes, MYF(MY_WME));
  if (mi_info != NULL)
    mi_close(mi_info);
  SET_STATE_TO_ERROR_AND_DBUG_VOID_RETURN;
}


/**
  Closes the MyISAM table.

  @return Operation status
    @retval backup::OK
    @retval backup::ERROR
*/

result_t Table_backup::end()
{
  DBUG_ENTER("myisam_backup::Table_backup::end");
  /* even if one close fails we still want to try the other one */
  if ((dfile_backup.close_file() != backup::OK) |
      (kfile_backup.close_file() != backup::OK))
    SET_STATE_TO_ERROR_AND_DBUG_RETURN;
  DBUG_RETURN(backup::OK);
}


Table_backup::~Table_backup()
{
  /* If all went well, we don't do anything here. */
  end();
}


/**
  Sends backup data for one table to the backup kernel.

  @param  buf             reference to Buffer where data should be put

  @return Operation status
    @retval backup::OK
    @retval backup::ERROR
*/

result_t Table_backup::get_data(Buffer &buf)
{
  result_t ret;
  DBUG_ENTER("myisam_backup::Table_backup::get_data");
  switch (in_file)
    {
    case DATA_FILE:
      ret= dfile_backup.get_data(buf);
      if (buf.last) // move to dumping the index file...
      {
        in_file= INDEX_FILE;
        buf.last= FALSE; // ... so this is not the last buffer on this stream
      }
      break;
    case INDEX_FILE:
      ret= kfile_backup.get_data(buf);
      break;
    default:
      DBUG_ASSERT(0);
      ret= backup::ERROR;
    };
  if (ret != backup::OK)
    SET_STATE_TO_ERROR_AND_DBUG_RETURN;
  DBUG_RETURN(ret);
}


/**
  Sends backup data for the log to the backup kernel.

  @param  buf             reference to Buffer where data should be put

  @return Operation status
    @retval backup::OK
    @retval backup::ERROR
*/

result_t Log_backup::get_data(Buffer &buf)
{
  result_t ret;
  DBUG_ENTER("myisam_backup::Log_backup::get_data");
  /*
    See, we detect a log write error encountered by the MyISAM myisam_log*
    and mi_log* functions, every time we read a packet from the log file.
  */
  if (((ret= log_file_backup.get_data(buf)) != backup::OK) ||
      (myisam_physical_log.hard_write_error_in_the_past == -1))
    SET_STATE_TO_ERROR_AND_DBUG_RETURN;
  DBUG_RETURN(backup::OK);
}


/**
  Closes a file in backup.

  @return Operation status
    @retval backup::OK
    @retval backup::ERROR
*/

result_t File_backup::close_file()
{
  int ret;
  if (fd < 0)
    return backup::OK;
  ret= my_close(fd, MYF(MY_WME));
  fd= -1;
  return ret ? backup::ERROR : backup::OK;
}


/**
  Sends backup data for a single file to the backup kernel.

  @param  buf             reference to Buffer where data should be put

  @return Operation status
    @retval backup::OK
    @retval backup::ERROR
*/

result_t File_backup::get_data(Buffer &buf)
{
  size_t    res, howmuch= buf.size;
  result_t  ret= backup::OK;

  DBUG_ENTER("myisam_backup::File_backup::get_data");

  buf.size= 1;
  DBUG_ASSERT(howmuch >= 2); // need at least 2 bytes
  *buf.data= static_cast<uchar>(file_code);
  howmuch--;

  if (backup_file_size >= file_size)
    res= 0; // we don't have to read/send the rest of file
  else
  {
    res= my_read(fd, buf.data + 1, howmuch, MYF(MY_WME));
    // DBUG_DUMP("sending",buf_ptr-1, 16);
  }
  if (res == (size_t)(-1))
  {
    ret= backup::ERROR;
    goto end;
  }
  backup_file_size+= res;
  if (res == 0) // end of file
  {
    buf.size= 0; // don't even send a packet
    buf.last= TRUE;
    goto end;
  }
  buf.size+= res;
  buf.last= FALSE;
end:
  DBUG_PRINT("info",("ret %d buf.last %d buf.size %u",
                     ret, buf.last, static_cast<uint>(buf.size)));
  DBUG_RETURN(ret);
}


/**************************************
 *
 *   RESTORE FUNCTIONALITY
 *
 **************************************/

class Object_restore;

/**
  Handles restore orders received from the backup kernel (implements the
  API).
*/
class Restore: public Restore_driver
{
public:
  Restore(const Table_list &tables);
  virtual ~Restore();
  virtual result_t  begin(const size_t);
  virtual result_t  end();
  virtual result_t  send_data(Buffer &buf);
  virtual result_t  cancel()
    {
      /* Nothing to do in cancel(); free() will suffice */
      return backup::OK;
    };
  virtual void      free() { delete this; };

private:
  enum { PUMPING, DONE, ERROR } state;
  uint            images_left; ///< how many images left to restore
  Object_restore  **images; ///< one for the log and one per table
  char restore_log_name[FN_REFLEN];
};


/** An object to restore; in practice, a table or the log */
class Object_restore
{
public:
  virtual result_t send_data(const Buffer &buf)= 0;
  virtual ~Object_restore() {};
  /**
    Closes the object, post_restore() can later be called. Whereas in
    Object_backup, closing is done in end() (there is no close()), here we
    have a dedicated close() method. This is because we must close tables and
    the log then apply the log then repair indices: we need to close way
    before end()).
  */
  virtual result_t close()= 0;
  /** Does additional restore operations between close() and end() */
  virtual result_t post_restore()= 0;
  bool internal_error() { return state == ERROR; }
  virtual result_t end()= 0; ///< cleanups
protected:
  enum { OK, ERROR } state;
};


/**
  An object to restore is made of one or more such files. This class does not
  open the file, user has to open it. This class provides a helper method if
  its user wants to close the file.
*/
class File_restore
{
public:
  File_restore() : fd(-1) {}
  void init(int fd_arg) { fd= fd_arg; }
  result_t send_data(const Buffer &);
  result_t close_file();
private:
  int fd; ///< file descriptor
};


/** Handles restoring a single table */
class Table_restore: public Object_restore, public Myisam_table_ref
{
public:
  Table_restore(const Table_ref &tbl);
  virtual result_t send_data(const Buffer &buf);
  virtual ~Table_restore();
  virtual result_t close();
  virtual result_t post_restore();
  virtual result_t end(); ///< cleanups
 private:
  File_restore dfile_restore, kfile_restore;
  bool         rebuild_index; ///< if we have to rebuild index or not
  THD          *thd; ///< rebuilding index requires a THD
};


/** Handles restoring the log */
class Log_restore: public Object_restore
{
public:
  Log_restore(const char *log_name_arg);
  virtual result_t send_data(const Buffer &buf);
  virtual ~Log_restore();
  virtual result_t close();
  virtual result_t post_restore();
  virtual result_t end();
private:
  const char *log_name;
  File_restore log_file_restore;
  bool log_deleted; ///< if we have already deleted the log or not
};


/**
  Creates a restore driver, per the backup API. @see backup::Engine.

  @return Operation status
    @retval backup::OK
    @retval backup::ERROR
*/

result_t Engine::get_restore(const version_t ver, const uint32,
                             const Table_list &tables, Restore_driver* &drv)
{
  if (ver > MYISAM_BACKUP_VERSION)
  {
    char errbuff[200];
    my_snprintf(errbuff, sizeof(errbuff),
                MYISAM_ERR(MYISAM_ERR_BACKUP_TOO_RECENT),
                ver, MYISAM_BACKUP_VERSION);
    my_error(ER_GET_ERRMSG, MYF(0),
             MYISAM_ERR_BACKUP_TOO_RECENT, errbuff, "MyISAM");
    return backup::ERROR;    
  }

  Restore *ptr= new Restore(tables);
  if (unlikely(!ptr))
    return backup::ERROR;
  drv= ptr;
  return backup::OK;
}


Restore::Restore(const Table_list &tables):
  Restore_driver(tables), state(ERROR), images_left(0), images(NULL)
{
  /* This constructor cannot fail otherwise begin() would have to detect it */
}


/**
  This destructor is only called by the class' free(). It cleans up any
  leftover the driver could have. It is safe to call it at any point. In a
  normal (no error) situation, it does nothing, all should already have been
  done by earlier stages.
*/

Restore::~Restore()
{
  DBUG_ENTER("myisam_backup::Restore::~Restore");
  if (images)
  {
    for (uint n= 0; n <= m_tables.count(); ++n)
      delete images[n];
    delete[] images;
  }
  DBUG_VOID_RETURN;
}


/**
  Sets MyISAM in a state ready for us to restore. I.e. creates a temporary
  file to host the log's restored copy.

  @return Operation status
    @retval backup::OK
    @retval backup::ERROR
*/

result_t Restore::begin(const size_t)
{
  THD *thd= current_thd;
  DBUG_ENTER("myisam_backup::Restore::begin");
  my_snprintf(restore_log_name, sizeof(restore_log_name),
	      "%s/%s%lx_%lx_%x-restorelog", mysql_tmpdir,
	      tmp_file_prefix, current_pid, thd->thread_id,
              thd->tmp_table++);
  unpack_filename(restore_log_name, restore_log_name);

  DBUG_ASSERT(m_tables.count() > 0); // or bug in the backup kernel
  images_left= 1 + m_tables.count();
  images= new Object_restore*[images_left];
  if (unlikely(!images))
    SET_STATE_TO_ERROR_AND_DBUG_RETURN;
  bzero(images, images_left * sizeof(*images));
  state= PUMPING;
  DBUG_RETURN(backup::OK);
}


/**
  If no error happened, we have to apply the log and possibly repair
  indexes; this has to be done here and not in the destructor (as it has to
  be done only in case of success, while a destructor runs in all cases).
  Because we have no "end of stream" notifications yet, when we come here all
  our tables/logs are opened. and log is not applied (both things which could
  be done in send_data() if we knew end-of-stream). Repairing indexes, on the
  other hand, really has to be done here.

  @todo selective restore (this is just passing a proper function which
  checks if the table is in a hash of tables).

  @return Operation status
    @retval backup::OK
    @retval backup::ERROR
*/

result_t Restore::end()
{
  DBUG_ENTER("myisam_backup::Restore::end");
  /*
    Rafal said currently end() is called in case of error but said he'll fix
    that (only free() will be called)
  */
  DBUG_ASSERT(state != ERROR);
  if (images)
  {
    for (uint n=0; n <= m_tables.count(); ++n)
      if (images[n] && images[n]->close() != backup::OK)
        SET_STATE_TO_ERROR_AND_DBUG_RETURN;

    /*
      Tables are closed. Apply backup log if it exists (it does not exist if
      it was empty at backup time), this is post_restore() of images[0]. Then
      repair indices if needed (post_restore() of other images).
    */
    for (uint n=0; n <= m_tables.count(); ++n)
      if (images[n] && images[n]->post_restore() != backup::OK)
        SET_STATE_TO_ERROR_AND_DBUG_RETURN;

    /*
      By doing here the work of the destructor we can test the return code of
      end(). We don't do it for tables as they will do nothing in end()
      (except freeing their memory) so that can be left to the destructor.
    */
    if (images[0] && images[0]->end() != backup::OK)
      SET_STATE_TO_ERROR_AND_DBUG_RETURN;
  }

  DBUG_RETURN(backup::OK);
}


/**
  Receives and restores data for tables and log from the backup kernel.

  @param  buf             reference to Buffer where data is

  @return Operation status
    @retval backup::OK
    @retval backup::ERROR
*/

result_t Restore::send_data(Buffer &buf)
{
  result_t ret;
  uint stream= buf.table_num;
  DBUG_ENTER("myisam_backup::Restore::send_data");
  DBUG_PRINT("enter",("Got packet with %u bytes from stream %d",
                      static_cast<uint>(buf.size), buf.table_num));

  if (state == DONE)
  {
    /* we never come here */
    DBUG_PRINT("info",("Ignoring the packet (all objects already restored)"));
    DBUG_RETURN(backup::DONE);
  }

  Object_restore *image= images[stream];

  /*
    We create an image when we see a new stream.
    Still we have N open tables during the last table's restore.
    But when Rafal implements that the last buffer of a stream has
    buf.last==TRUE (soon), we can close tables earlier.
  */
  if (!image)
  {
    if (stream >= 1)
      image= new Table_restore(m_tables[stream-1]);
    else
      image= new Log_restore(restore_log_name);
    images[stream]= image;
    if (unlikely(!image || image->internal_error()))
      SET_STATE_TO_ERROR_AND_DBUG_RETURN;
  }

  if ((ret= image->send_data(buf)) != backup::OK)
    SET_STATE_TO_ERROR_AND_DBUG_RETURN;

  /* for when we have "end of stream" notifications: */
#ifdef TODO_HAVE_END_OF_STREAM
  if (buf.last)
  {
    if (image->close() != backup::OK)
      SET_STATE_TO_ERROR_AND_DBUG_RETURN;
    images_left--;
    if (images_left == 0)
    {
      state= DONE;
      /* DONE means done with all send_data() calls, but we have more work */
      DBUG_RETURN(backup::DONE);
    }
  }
#endif

  DBUG_RETURN(backup::OK);
}



/**
  Restores the log.

  @param  log_name_arg    Name under which log should be created
*/

Log_restore::Log_restore(const char *log_name_arg) : log_name(log_name_arg)
{
  DBUG_ENTER("myisam_backup::Log_restore::Log_restore");
  int fd= my_create(log_name, 0, O_WRONLY, MYF(MY_WME));
  if (fd < 0)
  {
    log_deleted= TRUE;
    SET_STATE_TO_ERROR_AND_DBUG_VOID_RETURN;
  }
  log_deleted= FALSE;
  log_file_restore.init(fd);
  state= OK;
  DBUG_VOID_RETURN;
}


/**
  Closes the log.

  @return Operation status
    @retval backup::OK
    @retval backup::ERROR
*/

result_t Log_restore::close()
{
  DBUG_ENTER("myisam_backup::Log_restore::close");
  if (log_file_restore.close_file() != backup::OK)
    SET_STATE_TO_ERROR_AND_DBUG_RETURN;
  DBUG_RETURN(backup::OK);
}


/**
  Applies the log to restored tables, to make them consistent.

  @return Operation status
    @retval backup::OK
    @retval backup::ERROR
*/

result_t Log_restore::post_restore()
{
  MI_EXAMINE_LOG_PARAM mi_exl;
  DBUG_ENTER("myisam_backup::Log_restore::post_restore");
  mi_examine_log_param_init(&mi_exl);
  mi_exl.log_filename= log_name;
  mi_exl.update= 1;
  /*
    For max_files, the assumption is that at backup time the server had
    enough file descriptors and so should have that many now.
  */
  mi_exl.max_files= open_files_limit;
  if (mi_examine_log(&mi_exl))
    SET_STATE_TO_ERROR_AND_DBUG_RETURN;

  DBUG_RETURN(backup::OK);
}


/**
  Closes and deletes the log.

  @return Operation status
    @retval backup::OK
    @retval backup::ERROR
*/

result_t Log_restore::end()
{
  DBUG_ENTER("myisam_backup::Log_restore::end");
  /* log is applied so we don't need it anymore */
  if (close() != backup::OK ||
      (!log_deleted && my_delete(log_name, MYF(MY_WME))))
    SET_STATE_TO_ERROR_AND_DBUG_RETURN;
  log_deleted= TRUE;
  DBUG_RETURN(backup::OK);
}


Log_restore::~Log_restore()
{
  /* If all went well, we don't do anything here. */
  end();
}


/** Opens a MyISAM table for restoring it */

Table_restore::Table_restore(const Table_ref &tbl):
  Myisam_table_ref(tbl), rebuild_index(FALSE)
{
  MI_INFO *mi_info;
  File dfiledes= -1, kfiledes= -1;
  DBUG_ENTER("myisam_backup::Table_restore::Table_restore");
  DBUG_PRINT("enter",("Initializing backup image for table %s",
                      file_name.ptr()));
  /*
    Here we use low-level mi_* functions as all we want is a pair of file
    descriptors.
    Though we only want to write (O_WRONLY), the SQL layer uses only O_RDONLY
    and O_RDWR, so here we don't try to be original.
  */
  mi_info= mi_open(file_name.ptr(), O_RDWR, 0);
  if (!mi_info)
  {
    /* table does not exist or is corrupted? not normal, it's just created */
    goto err;
  }
  /*
    It's ok to copy the kfile descriptor and write() to it as the upper layers
    guarantee that we are the only user of the brand new table (nobody will
    lseek() under our feet).
  */
  if (((dfiledes= my_dup(mi_info->dfile, MYF(MY_WME))) < 0) ||
      ((kfiledes= my_dup(mi_info->s->kfile, MYF(MY_WME))) < 0))
    goto err;
  /*
    We are going to my_write() to the files without updating the table's
    state (mi_info->state). If we called mi_close() only at end of restore,
    that function may write its out-of-date state on the table.
  */
  mi_close(mi_info);
  mi_info= NULL;
  /* seek them at start, because we use my_write() */
  if ((my_seek(dfiledes, 0, SEEK_SET, MYF(MY_WME)) == MY_FILEPOS_ERROR) ||
      (my_seek(kfiledes, 0, SEEK_SET, MYF(MY_WME)) == MY_FILEPOS_ERROR))
    goto err;
  dfile_restore.init(dfiledes);
  kfile_restore.init(kfiledes);
  thd= current_thd;
  state= OK;
  DBUG_VOID_RETURN;
err:
  if (dfiledes > 0)
    my_close(dfiledes, MYF(MY_WME));
  if (kfiledes > 0)
    my_close(kfiledes, MYF(MY_WME));
  if (mi_info != NULL)
    mi_close(mi_info);
}


/**
  Closes a table.

  @return Operation status
    @retval backup::OK
    @retval backup::ERROR
*/

result_t Table_restore::close()
{
  DBUG_ENTER("myisam_backup::Table_restore::close");
  DBUG_PRINT("info",("table: %s", file_name.ptr()));
  if ((dfile_restore.close_file() != backup::OK) |
      (kfile_restore.close_file() != backup::OK))
    SET_STATE_TO_ERROR_AND_DBUG_RETURN;

  /*
    CAUTION! Ugliest hack ever!
    This hack tries to recover from bypassing the MyISAM interface
    by the MyISAM restore driver.
    The situation is so:
    The backup kernel opens and locks the tables in backup.
    But the MyISAM restore driver does not use the open MI_INFO
    instance. Instead it opens another instance, duplicates its
    file descriptors, and closes the instance. Then it uses the
    duplicate file descriptors to write directly ("physically")
    to the data and index files.
    Among the writes are chunks of data from the index file, which
    overwrite the index header with the state info.
    In this function, called after all data have been written, the
    duplicate file descriptors are closed (above). Now the index
    and data files have the contents they ought to have.
    Everything would be fine if no instance of the table would be
    open at the time. Then a new open would read all table info from
    disk and everybody would be happy.
    However, the backup kernel still has the table open. Parts of
    the index file are cached in the open MYISAM_SHARE object.
    If the backup kernel would close the tables, this old information
    would be written to the index file, which crashes the table.
    This hack tries to solve the problem by loading the share with
    information from the index file. At first, we open a new MI_INFO
    instance from the table. This open does not read the state info
    from the file because another instance is already open from the
    same table. But the open gives us access to the share.
    We do then explicitly call mi_state_info_read_dsk(), which is
    the function that loads the share from the index file at an
    initial open. Well, not exactly. At open a similar function is
    used, after the index header has been read by a direct read.
    But the mentioned function includes both, read and share load.
    Another small problem is that the function doesn't do anything
    if external locking is disabled. It assumes that no external
    (or bypassing) writes happen to the files. Since we did exactly
    this, we must pretend that we are doing external locking. The
    function uses the variable 'myisam_single_user' for the
    decision. So we temporarily change it.
    Now we can close the new table instance. This won't write the
    state again, because is is not the last open instance.
    But since the share does now cache the new values from the
    index file, the backup kernel's close writes the correct
    information back to the file.
  */
  {
    MI_INFO      *mi_info;
    MYISAM_SHARE *share;
    my_bool      old_myisam_single_user;

    mi_info= mi_open(file_name.ptr(), O_RDWR, HA_OPEN_FOR_REPAIR);
    if (mi_info == NULL)
      goto err;
    share= mi_info->s;
    DBUG_PRINT("myisam_backup", ("share data_file: %lu",
                                 (ulong) share->state.state.data_file_length));
    old_myisam_single_user= myisam_single_user;
    myisam_single_user= FALSE;
    if (mi_state_info_read_dsk(share->kfile,&share->state,1))
    {
      myisam_single_user= old_myisam_single_user;
      goto err;
    }
    myisam_single_user= old_myisam_single_user;
    DBUG_PRINT("myisam_backup", ("share data_file: %lu",
                                 (ulong) share->state.state.data_file_length));
    /*
      Now follows the most dirty part of the hack.
      We have correct information in the share, but the instance that
      holds the lock on the table has a local copy of the state.
      We must find this instance and fix the local info.
      Fortunately there is a state pointer, which can be set to the
      share. This invalidates the instance's local copy.
      We need to acquire share->intern_lock when traversing the list
      of open MyISAM instances.
    */
    {
      LIST *list_element ;
      pthread_mutex_lock(&share->intern_lock);
      for (list_element= myisam_open_list;
           list_element;
           list_element= list_element->next)
      {
        MI_INFO *tmpinfo= (MI_INFO*) list_element->data;
        if (tmpinfo->s == share)
          tmpinfo->state= &share->state.state;
      }
      pthread_mutex_unlock(&share->intern_lock);
    }
    if (mi_close(mi_info))
      goto err;
    goto end;

  err:
    SET_STATE_TO_ERROR_AND_DBUG_RETURN;

  end :
    do {} while (0); /* Empty statement, syntactically required. */
  }

  DBUG_RETURN(backup::OK);
}


/**
  Closes a table.

  @return Operation status
    @retval backup::OK
    @retval backup::ERROR
*/

result_t Table_restore::end()
{
  return close();
}


Table_restore::~Table_restore()
{
  end();
}


/**
  Repairs table's index if needed. Has to be done after applying the log.

  @return Operation status
    @retval backup::OK
    @retval backup::ERROR
*/

result_t Table_restore::post_restore()
{
  HA_CHECK_OPT check_opt;
  TABLE *table= NULL;
  int error;
  Vio* save_vio;
  DBUG_ENTER("myisam_backup::Table_restore::post_restore");

  if (!rebuild_index)
  {
    MI_INFO *mi_info;
    MYISAM_SHARE *share;
    /*
      Table was copied while it was possibly open by other clients; we need to
      correct open_count to not trigger superfluous warning messages or repair
      by --myisam-recover. If we rebuild the index, that will automatically
      fix open_count.
    */
    mi_info= mi_open(file_name.ptr(), O_RDWR, HA_OPEN_FOR_REPAIR);
    if ((error= (mi_info == NULL)))
      goto err;
    share= mi_info->s;
    if (share->state.changed & STATE_BAD_OPEN_COUNT)
    {
      /* table already had a problem when backup started, leave open_count */
      DBUG_PRINT("info", ("STATE_BAD_OPEN_COUNT is on"));
    }
    else
    {
      /* open_count>0 only because we copied while open, no problem */
      share->state.open_count= 0;
      /* force this correct open_count to disk */
      error= mi_state_info_write(share, share->kfile, &share->state, 1);
    }
    error|= mi_close(mi_info);
    goto err;
  }

  /*
    myisamchk() as well as ha_myisam::repair() do a lot of operations before
    and after mi_repair(); to not duplicate code we reuse one of them.
    As we are in the server here, we use the one of the server.
    A "new ha_myisam + ha_open()" is not sufficient as TABLE and TABLE_SHARE
    are needed for ha_myisam::open(). So we use open_temporary_table() which
    sets up all fine without touching thread's structure (and so, without
    causing problems to locks, without interfering with close_thread_tables()
    which would be done by another driver in the same thread etc).
    Note that as the table has just been created, and in theory is protected
    from any usage, by the upper backup layer, opening it with
    open_temporary_table() is correct.
  */
  char path[FN_REFLEN];
  build_table_filename(path, sizeof(path), db.ptr(), name.ptr(), "", 0);
  table= open_temporary_table(thd, path, db.ptr(), name.ptr(),
                              false, OTM_OPEN);

  if ((error= (!table || !table->file)))
    goto err;

  check_opt.init();
  check_opt.flags|= T_VERY_SILENT | T_QUICK;
  /*
    We do not want repair() to spam us with messages (protocol->store() etc).
    Just send them to the error log, and report the failure in case of
    problems.
    Note that ha_myisam::restore() does not do that (merely uses the same
    check_opt.flags as us), as it is allowed to return an array of errors.
  */
  save_vio= thd->net.vio;
  thd->net.vio= NULL;
  error= table->file->ha_repair(thd,&check_opt) != 0;
  thd->net.vio= save_vio;

err:
  if (table)
  {
    intern_close_table(table);
    my_free(table, MYF(MY_WME));
  }
  if (error)
    SET_STATE_TO_ERROR_AND_DBUG_RETURN;
  DBUG_RETURN(backup::OK);
}


/**
  Receives and restores data for one table from the backup kernel.

  @param  buf             reference to Buffer where data is

  @return Operation status
    @retval backup::OK
    @retval backup::ERROR
*/

result_t Table_restore::send_data(const Buffer &buf)
{
  enum enum_file_code file_code= static_cast<enum enum_file_code>(*buf.data);
  result_t ret;
  DBUG_ENTER("myisam_backup::Table_restore::send_data");

  switch (file_code)
  {
  case DATA_FILE_CODE:
    ret= dfile_restore.send_data(buf);
    break;
  case HEADER_INDEX_FILE_CODE:
    rebuild_index= TRUE; // because we are given only the index's header
    // fall through
  case WHOLE_INDEX_FILE_CODE:
    ret= kfile_restore.send_data(buf);
    break;
  default:
    DBUG_PRINT("info",("packet with code %d I didn't expect", file_code));
    DBUG_ASSERT(0);
    ret= backup::ERROR;
  }
  if (ret != backup::OK)
    SET_STATE_TO_ERROR_AND_DBUG_RETURN;
  DBUG_RETURN(ret);
}


/**
  Receives and restores data for the log from the backup kernel.

  @param  buf             reference to Buffer where data is

  @return Operation status
    @retval backup::OK
    @retval backup::ERROR
*/

result_t Log_restore::send_data(const Buffer &buf)
{
  enum enum_file_code file_code= static_cast<enum enum_file_code>(*buf.data);
  result_t ret;
  DBUG_ENTER("myisam_backup::Log_restore::send_data");

  ret= (file_code == LOG_FILE_CODE) ? log_file_restore.send_data(buf) :
    backup::ERROR;
  if (ret != backup::OK)
    SET_STATE_TO_ERROR_AND_DBUG_RETURN;
  DBUG_RETURN(ret);
}


/**
  Receives and restores data for one single file from the backup kernel.

  @param  buf             reference to Buffer where data is

  @return Operation status
    @retval backup::OK
    @retval backup::ERROR
*/

result_t File_restore::send_data(const Buffer &buf)
{
  size_t howmuch= buf.size;

  DBUG_ENTER("myisam_backup::File_restore::send_data");
  //DBUG_DUMP("receiving",buf.data + 1, 16);

  // We should receive same buffers as those made at backup time
  DBUG_ASSERT(howmuch >= 2);
  howmuch--; // skip the first byte which contains the code
  size_t res= my_write(fd, buf.data +1, howmuch, MYF(MY_WME));

  DBUG_RETURN((res != howmuch) ? backup::ERROR : backup::OK);
}


/**
  Closes a file in restore.

  @return Operation status
    @retval backup::OK
    @retval backup::ERROR
*/

result_t File_restore::close_file()
{
  int ret;
  if (fd < 0)
    return backup::OK;
  ret= my_close(fd, MYF(MY_WME));
  fd= -1;
  return ret ? backup::ERROR : backup::OK;
}


} // myisam_backup namespace


/**
  Returns the backup Engine used by this storage engine, per the API.

  @return Operation status
    @retval backup::OK
    @retval backup::ERROR
*/

Backup_result_t myisam_backup_engine(handlerton *self, Backup_engine* &be)
{
  be= new myisam_backup::Engine();

  if (unlikely(!be))
    return backup::ERROR;

  return backup::OK;
}
