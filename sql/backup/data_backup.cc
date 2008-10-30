/**
  @file

  Code used to backup table data.

  Function @c write_table_data() and @c restore_table_data() use backup/restore
  drivers and protocols to create snapshot of the data stored in the tables being
  backed up.

  @todo Implement better scheduling strategy in Scheduler::step
  @todo Add error reporting in the scheduler and elsewhere
  @todo If an error from driver is ignored (and operation retried) leave trace
        of the error in the log.
 */

#include "../mysql_priv.h"

#include "backup_kernel.h"
#include "backup_engine.h"
#include "stream.h"
#include "be_default.h"  // needed for table locking code

/***********************************************

                  DATA BACKUP

 ***********************************************/

namespace backup {

struct backup_state {

 /// State of a single backup driver.
 enum value { INACTIVE,   ///< Before backup process is started (phase 0).
              INIT,       ///< During initial data transfer (phase 1).
              WAITING,    ///< Waiting for other drivers to finish init phase (phase 2).
              PREPARING,  ///< Preparing for @c lock() call (phase 3).
              READY,      ///< Ready for @c lock() call (phase 4)
              FINISHING,  ///< Final data transfer (phase 7).
              DONE,       ///< Backup complete.
              SHUT_DOWN,  ///< After @c end() call.
              CANCELLED,  ///< After cancelling backup process.
              ERROR,
              MAX };

#ifndef DBUG_OFF

  static const char* name[];

  struct Initializer
  {
    Initializer()
    {
      name[INACTIVE]=   "INACTIVE";
      name[INIT]=       "INIT";
      name[WAITING]=    "WAITING";
      name[PREPARING]=  "PREPARING";
      name[READY]=      "READY";
      name[FINISHING]=  "FINISHING";
      name[DONE]=       "DONE";
      name[SHUT_DOWN]=  "SHUT DOWN";
      name[CANCELLED]=  "CANCELLED";
      name[ERROR]=      "ERROR";
    }
  };

 private:

  static Initializer init;

#endif

};

#ifndef DBUG_OFF

const char* backup_state::name[backup_state::MAX];
backup_state::Initializer init;

#endif

/**
  Used to write data blocks to a stream.

  This class defines how buffers are allocated for data transfers
  (@c get_buf() method). Each block is written as a separate chunk of data.
 */
class Block_writer
{
 public:

  enum result_t { OK, NO_RES, ERROR };

  result_t  get_buf(Buffer &);
  result_t  write_buf(const Buffer&);
  void      drop_buf(Buffer&);

  Block_writer(byte, size_t, Output_stream&);
  ~Block_writer();

 private:

  byte           snap_num;  ///< snapshot to which the data belongs
  Output_stream  &m_str;    ///< stream to which we write
  size_t         buf_size;  ///< size of a single data block
  byte           *data_buf; ///< pointer to data buffer
  bool           taken;     ///< flag which indicates that the buffer is in use

  friend class Backup_pump;
};

/**
  @class Backup_pump

  Poll backup driver for backup data and send it to a stream. Monitors stages
  of the backup process, keeps track of closed streams etc.

  Usage: Initialize using @c begin() method, then call @c pump() method
  repeatedly. The state @c member informs about the current state of the backup
  process. When done, call @c end() method. Methods @c prepare(), @c lock() and
  @c unlock() are forwarded to backup driver to implement multi-engine
  synchronization.
*/

class Backup_pump
{
 public:

  backup_state::value  state; ///< State of the backup driver.

  enum { READING, ///< Pump is polling driver for data.
         WRITING  ///< Pump sends data to the stream.
       } mode;

  /** The estimate returned by backup driver's @c init_data() method. */
  size_t  init_size;
  size_t  bytes_in, bytes_out;

  const char *m_name; ///< Name of the driver (for debug purposes).

  Backup_pump(Snapshot_info&, Block_writer&);
  ~Backup_pump();

  bool is_valid()
  { return m_drv && state != backup_state::ERROR; }

  int pump(size_t*);

  int begin();
  int end();
  int prepare();
  int lock();
  int unlock();
  int cancel();

  /// Return the backup driver used by the pump.
  Backup_driver &drv() const
  {
    DBUG_ASSERT(m_drv);
    return *m_drv;
  }

  void set_logger(Logger *log)
  { m_log= log; }

 private:

  /// If block writer has no buffers, retry this many times before giving up.
  static const uint get_buf_retries= 3; 

  Logger        *m_log;   ///< Used to report errors if not NULL.
  Backup_driver *m_drv;   ///< Pointer to the backup driver.
  Block_writer  &m_bw;    ///< Block writer used for writing data blocks.
  Buffer        m_buf;    ///< Buffer used for data transfers.

  /**
    Pointer to the memory area used as write buffer.

    If m_buf_head is NULL and we are in READ state, then we should allocate new 
    write buffer and ask driver to fill it with data. M_buf_head is not NULL 
    only when the write buffer is being filled with data but the operation is 
    not complete.
   */
  byte          *m_buf_head;

  /// How many times failed to get a buffer from block writer.
  uint          m_buf_retries;

  /// Bitmap showing which streams have been closed by the driver.
  MY_BITMAP     m_closed_streams;

  void mark_stream_closed(uint stream_num)
  {
    bitmap_set_bit(&m_closed_streams, stream_num);
  }

  bool all_streams_closed()
  {
     return bitmap_is_set_all(&m_closed_streams);
  }

};

/*
  The constant is declared here (and memory allocated for it) because
  IBM's xlc compiler requires that. However, the intention was to make it
  a pure symbolic constant (no need to allocate memory). If someone knows
  how to achieve that and keep xlc happy, please let me know. /Rafal
*/ 
const uint Backup_pump::get_buf_retries;

/**
 @class Scheduler

 Used to drive several backup pumps in a fair fashion. Also, keeps track of the
 state of these pumps.

*/
class Scheduler
{
  class Pump;
  class Pump_iterator;

 public:

  int add(Pump*);
  int step();

  int prepare();
  int lock();
  int unlock();

  uint  init_count;     ///< no. drivers sending init data
  uint  prepare_count;  ///< no. drivers preparing for lock
  uint  finish_count;   ///< no. drivers sending final data

  size_t init_left() const
  { return m_known_count? m_init_left/m_known_count + 1 : 0; }

  size_t bytes_written() const
  { return m_total; }

  bool is_empty() const
  { return m_count == 0; }

  ~Scheduler() { cancel_backup(); }

 private:

  LIST   *m_pumps, *m_last;
  Logger *m_log;        ///< used to report errors if not NULL
  uint   m_count;       ///< current number of pumps
  size_t m_total;       ///< accumulated position of all drivers
  size_t m_init_left;   ///< how much of init data is left (estimate)
  uint   m_known_count; ///< no. drivers which can estimate init data size
  Output_stream &m_str; ///< stream to which we write
  bool   cancelled;     ///< true if backup process was cancelled

  Scheduler(Output_stream &s, Logger *log)
    :init_count(0), prepare_count(0), finish_count(0),
    m_pumps(NULL), m_last(NULL), m_log(log),
    m_count(0), m_total(0), m_init_left(0), m_known_count(0),
    m_str(s), cancelled(FALSE)
  {}

  void move_pump_to_end(const Pump_iterator&);
  void remove_pump(Pump_iterator&);
  void cancel_backup();

  friend int write_table_data(THD*, Backup_info&, Output_stream&);
  friend class Pump_iterator;
};

/**
  Extend Backup_pump with information about its position relative
  to other pumps.
 */
class Scheduler::Pump: public Backup_pump
{
  size_t start_pos;
  Block_writer bw;

  friend class Scheduler;

 public:

  Pump(Snapshot_info &snap, Output_stream &s)
    :Backup_pump(snap, bw), start_pos(0),
    bw(snap.m_num - 1, DATA_BUFFER_SIZE, s)
  {
    DBUG_ASSERT(snap.m_num > 0);
  }

  size_t pos() const
  { return start_pos + bytes_in; }
};


/**
   Commit Blocker

   The commit blocker ensures storage engines can't commit while in the
   synchronization phase of Online Backup algorithm. This is needed to
   make sure that:

     1) backups are consistent between engines, and
     2) that the binlog position is consistent with the engine images.

   REQUREMENTS
     A transactional engine needs to block commits during the locking phase
   of backup kernel.  It can not block DDL for long time.

   ALGORITHM
     The algortihm is implemented using five steps.

     1) Preventing new write locks on tables -- lock_global_read_lock()

        The idea is that non-transactional tables should not be modified.
        It is also a prerequisite of step 3.

     2) Wait for existing locks to unlock -- close_cached_tables()

        Ensures that no non-transactional table has any write lock. And thus
        no non-transactional engine can change any data (Note that the global
        read lock from step 1 is still in effect).

     3) Prevents new commits and waits for running commits to finish --
        make_global_read_lock_block_commit()

        This will make it impossible to enter commit phase in any transaction.
        This will also wait for any ongoing commit to finish.
        When the function returns, no transaction is in its commit phase.

     4) Read binlog position & do lock calls to all backup drivers

        This step will read the binlog position and save it in the backup_info
        structure. This will occur between the lock() and unlock() calls in
        the kernel.

     5) unlock_global_read_lock()

        This step unlocks the global read lock and thereby terminating the
        commit blocker.
  */


/**
   Block commits

   This method is used to initiate the first three steps of the commit blocker
   algorithm (global read lock, close cached tables, make global read lock
   block commits).

   @param  thd    (in) the current thread structure.
   @param  tables (in) list of tables to be backed-up.

   @returns 0 on success.
  */
int block_commits(THD *thd, TABLE_LIST *tables)
{
  DBUG_ENTER("block_commits()");

  /*
    Step 1 - global read lock.
  */
  DEBUG_SYNC(thd, "before_commit_block");
  if (lock_global_read_lock(thd))
    DBUG_RETURN(1);

  /*
    Step 2 - close cached tables.

    Notice to Online Backup developers.

    The method "close_cached_tables" as originally included in the commit
    blocker algorithm (see above) was omitted because there are no non-
    transactional tables that are not included in the existing default,
    consistent snapshot, and myisam drivers. This is only needed for engines
    that do not take lock tables. Thus, it should apply to write locked
    tables only and only to non-transactional engines.

    result= close_cached_tables(thd, 0, tables);
  */

  /*
    Step 3 - make the global read lock to block commits.
  */
  if (make_global_read_lock_block_commit(thd))
  {
    /* Don't leave things in a half-locked state */
    unlock_global_read_lock(thd);
    DBUG_RETURN(1);
  }
  DBUG_RETURN(0);
}

/**
   Unblock commits

   This method is used to terminate the commit blocker. It calls the last
   step of the algorithm (unlock global read lock).

   @param  thd    (in) the current thread structure.

   @returns 0
  */
int unblock_commits(THD *thd)
{
  DBUG_ENTER("unblock_commits()");
  unlock_global_read_lock(thd);
  DBUG_RETURN(0);
}

/**
  Save data from tables being backed up.

  Function initializes and controls backup drivers which create the image
  of table data. Currently single thread is used and drivers are polled in
  a round robin fashion.

  @returns 0 on success.
 */
int write_table_data(THD* thd, Backup_info &info, Output_stream &s)
{
  DBUG_ENTER("backup::write_table_data");

  if (info.snap_count() == 0 || info.table_count() == 0) // nothing to backup
    DBUG_RETURN(0);

  Scheduler   sch(s, &info.m_ctx);          // scheduler instance
  List<Scheduler::Pump>  inactive;  // list of images not yet being created

  // keeps maximal init size for images in inactive list
  size_t      max_init_size=0;

  time_t      vp_time;              // to store validity point time

  DBUG_PRINT("backup_data",("initializing scheduler"));

  // add unknown "at end" drivers to scheduler, rest to inactive list

  for (uint n=0; n < info.snap_count(); ++n)
  {
    Snapshot_info *i= info.m_snap[n];

    if (!i)
      continue;

    Scheduler::Pump *p= new Scheduler::Pump(*i, s);

    if (!p || !p->is_valid())
    {
      info.m_ctx.fatal_error(ER_OUT_OF_RESOURCES);
      goto error;
    }

    size_t init_size= p->init_size;

    if (init_size == Driver::UNKNOWN_SIZE)
    {
      if (sch.add(p))
        goto error;
    }
    else
    {
      if (init_size > max_init_size)
        max_init_size= init_size;

      if (inactive.push_back(p))
      {
        /* Allocation failed. 
           Error has been reported, but not logged to backup logs.
        */
        info.m_ctx.log_error(ER_OUT_OF_RESOURCES);
        goto error;
      }
    }
  }

  /*
    Each driver should be either in the scheduler or on inactive list.
   */
  DBUG_ASSERT( !sch.is_empty() || !inactive.is_empty() );

  DBUG_PRINT("backup_data",("%u drivers initialized, %u inactive",
                            sch.init_count,
                            inactive.elements));

  DBUG_PRINT("backup_data",("-- INIT PHASE --"));
  DEBUG_SYNC(thd, "before_backup_data_init");

  /*
   Poll "at end" drivers activating inactive ones on the way.

   Note: if scheduler is empty and there are images with non-zero
   init size (max_init_size > 0) then enter the loop as one such image
   will be added to the scheduler inside.
  */

  while (sch.init_count > 0 || sch.is_empty() && max_init_size > 0)
  {

    // add inactive image if it is a time for it

    if (max_init_size > 0 && sch.init_left() <= max_init_size)
    {
      List_iterator<Scheduler::Pump>  it(inactive);
      Scheduler::Pump *p;

      size_t second_max= 0;
      max_init_size= 0;
      Scheduler::Pump *p1= NULL;

      while ((p= it++))
      {
        if ( p->init_size >= max_init_size )
        {
          second_max= max_init_size;
          max_init_size= p->init_size;
          p1= p;
        }
      }

      max_init_size= second_max;

      if (sch.add(p1))
        goto error;
    }

    // poll drivers

    if (sch.step())
      goto error;
  }

  {
    // start "at begin" drivers
    DBUG_PRINT("backup_data",("- activating \"at begin\" drivers"));

    List_iterator<Scheduler::Pump>  it1(inactive);
    Scheduler::Pump *p;

    while ((p= it1++))
    if (sch.add(p))
      goto error;

    while (sch.init_count > 0)
    if (sch.step())
      goto error;

    // prepare for VP
    DBUG_PRINT("backup_data",("-- PREPARE PHASE --"));
    DEBUG_SYNC(thd, "before_backup_data_prepare");

    /*
      Note: block_commits is performed here because of the global read
      lock/table lock deadlock reported in bug#39602. It should be
      moved back to right before sch.lock() once a refined commit
      blocker has been implemented. WL#4610 tracks the work on a
      refined commit blocker
    */
    /*
      Block commits.

      TODO: Step 2 of the commit blocker has been skipped for this release.
      When it is included, developer needs to build a list of all of the
      non-transactional tables and pass that to block_commits().
    */
    int error= 0;
    error= block_commits(thd, NULL);
    if (error)
      goto error;

    if (sch.prepare())
      goto error;

    while (sch.prepare_count > 0)
    if (sch.step())
      goto error;

    /**** VP creation (start) ********************************************/
    
    DBUG_PRINT("backup_data",("-- SYNC PHASE --"));

    LOG_INFO binlog_pos;
    
    info.m_ctx.report_state(BUP_VALIDITY_POINT);
    /*
      This breakpoint is used to assist in testing state changes for
      the backup progress. It is not to be used to indicate actual
      timing of the validity point.
    */
    DEBUG_SYNC(thd, "after_backup_validated");
    
    /*
      Refined commit blocker should be set here; see WL#4610
    */

    DEBUG_SYNC(thd, "before_backup_data_lock");
    if (sch.lock())
      goto error;

    /*
      Save binlog information for point in time recovery on restore.
    */
    if (mysql_bin_log.is_open())
      if (mysql_bin_log.get_current_log(&binlog_pos))
      {
        info.m_ctx.fatal_error(ER_BACKUP_BINLOG);
        goto error;
      }

    /*
      If we are a connected slave, write master's binlog information to
      the progress log for later use.
    */
    st_bstream_binlog_pos master_pos;
    master_pos.pos= 0;
    if (obs::is_slave() && active_mi)
    {
      master_pos.pos= (ulong)active_mi->master_log_pos;
      master_pos.file= active_mi->master_log_name;
    }

    /*
      Save VP creation time.
    */
    vp_time= my_time(0);

    DEBUG_SYNC(thd, "before_backup_data_unlock");
    if (sch.unlock())
      goto error;

    /*
      Unblock commits.
    */
    DEBUG_SYNC(thd, "before_backup_unblock_commit");
    error= unblock_commits(thd);
    if (error)
      goto error;

    // Report and save information about VP

    info.save_vp_time(vp_time);
    info.m_ctx.report_vp_time(vp_time, TRUE); // TRUE = also write to progress log

    if (mysql_bin_log.is_open())
    {
      info.save_binlog_pos(binlog_pos);
      info.m_ctx.report_binlog_pos(info.binlog_pos);
    }

    /*
      If we are a slave and the master's binlog position has been recorded
      write it to the log.
    */
    if (obs::is_slave() && master_pos.pos)
      info.m_ctx.report_master_binlog_pos(master_pos);

    info.m_ctx.report_state(BUP_RUNNING);
    DEBUG_SYNC(thd, "after_backup_binlog");

    /**** VP creation (end) ********************************************/

    // get final data from drivers
    DBUG_PRINT("backup_data",("-- FINISH PHASE --"));

    DEBUG_SYNC(thd, "before_backup_data_finish");
    while (sch.finish_count > 0)
    if (sch.step())
      goto error;

    DBUG_PRINT("backup_data",("-- DONE --"));
  }

  info.data_size= sch.bytes_written();

  DBUG_RETURN(0);

 error:

  DBUG_RETURN(ERROR);
}

} // backup namespace

/**************************************************

        Implementation of Scheduler

 **************************************************/

namespace backup {

/**
  Used to iterate over backup pumps of a scheduler.
 */
class Scheduler::Pump_iterator
{
 public:

  LIST  *el;

  Pump* operator->()
  {
    return el? static_cast<Pump*>(el->data) : NULL;
  }

  void  operator++()
  {
    if(el) el= el->next;
  }

  operator bool() const
  { return el && el->data; }

  void operator=(const Pump_iterator &p)
  { el= p.el; }

  Pump_iterator(): el(NULL)
  {}

  Pump_iterator(const Scheduler &sch) :el(sch.m_pumps)
  {}

};

/**
  Pick next backup pump and call its @c pump() method.

  Method updates statistics of number of drivers in each phase which is used
  to detect end of a backup process.
 */
int Scheduler::step()
{
  // Pick next driver to pump data from.

  Pump_iterator p(*this);

  /*
    An attempt to implement more advanced scheduling strategy (not working).

    for (Pump_ptr it(m_pumps); it; ++it)
      if ( !p || it->pos() < p->pos() )
        p= it;
  */

  if (!p) // No active pumps
  {
    init_count= prepare_count= finish_count= 0;  // safety
    return 0;
  }

  move_pump_to_end(p);

  DBUG_PRINT("backup_data",("polling %s", p->m_name));

  backup_state::value before_state= p->state;

  size_t howmuch;
  int res= p->pump(&howmuch);

  backup_state::value after_state= p->state;

  // The error state should be set iff error was reported
  DBUG_ASSERT(!(res && after_state != backup_state::ERROR));
  DBUG_ASSERT(!(!res && after_state == backup_state::ERROR));

  // update statistics

  if (!res && howmuch > 0)
  {
    m_total += howmuch;

    if (before_state == backup_state::INIT
        && p->init_size != Driver::UNKNOWN_SIZE)
      m_init_left -= howmuch;
  }

  if (after_state != before_state)
  {
    switch (before_state) {

    case backup_state::INIT:
      if (init_count > 0)
        init_count--;
      break;

    case backup_state::PREPARING:
      if (prepare_count > 0)
        prepare_count--;
      break;

    case backup_state::FINISHING:
      if (finish_count > 0)
        finish_count--;
      break;

    default: break;
    }

    switch (after_state) {

    case backup_state::INIT:
      init_count++;
      break;

    case backup_state::PREPARING:
      prepare_count++;
      break;

    case backup_state::FINISHING:
      finish_count++;
      break;

    case backup_state::DONE:
      res= p->end();  // Logs errors, fall-through to error handling below

    case backup_state::ERROR:
      remove_pump(p);   // Note: never errors.
      if (res)
        cancel_backup(); // we hit an error - bail out
                         // Note: cancel_backup() never errors.
      break;

    default: break;
    }

    DBUG_PRINT("backup_data",("driver counts: total=%u, init=%u, prepare=%u, finish=%u.",
                              m_count, init_count, prepare_count, finish_count));
  }

  return res;
}

/**
  Add backup pump to the scheduler.

  The pump is initialized with begin() call. In case of error, it is deleted.
 */
int Scheduler::add(Pump *p)
{
  size_t  avg= m_count? m_total/m_count + 1 : 0;

  if (!p)  // no pump to add
    return 0;

  p->set_logger(m_log);
  p->start_pos= avg;

  if (p->begin())
    goto error;

  // in case of error, above call should return non-zero code (and report error)
  DBUG_ASSERT(p->state != backup_state::ERROR);

  DBUG_PRINT("backup_data",("Adding %s to scheduler (at pos %lu)",
                            p->m_name, (unsigned long)avg));

  m_pumps= list_cons(p, m_pumps);
  if (!m_last)
    m_last= m_pumps;

  m_count++;
  m_total += avg;

  if (p->init_size != Driver::UNKNOWN_SIZE)
  {
    m_init_left += p->init_size;
    m_known_count++;
  }

  switch (p->state) {

  case backup_state::INIT:
    init_count++;
    break;

  case backup_state::PREPARING:
    prepare_count++;
    break;

  case backup_state::FINISHING:
    finish_count++;
    break;

  default: break;
  }

  DBUG_PRINT("backup_data",("driver counts: total=%u, init=%u, prepare=%u, finish=%u.",
                            m_count, init_count, prepare_count, finish_count));
  DBUG_PRINT("backup_data",("total init data size estimate: %lu",
                            (unsigned long)m_init_left));

  return 0;

 error:

  delete p;
  cancel_backup();
  return ERROR;
}

/// Move backup pump to the end of scheduler's list.
void Scheduler::move_pump_to_end(const Pump_iterator &p)
{
  // The pump to move is in the m_pumps list so the list can't be empty.
  DBUG_ASSERT(m_pumps);
  if (m_last != p.el)
  {
    m_pumps= list_delete(m_pumps, p.el);
    m_last->next= p.el;
    p.el->prev= m_last;
    p.el->next= NULL;
    m_last= p.el;
  }
}

/**
  Remove backup pump from the scheduler.

  The corresponding backup driver is shut down using @c end() call.
 */
void Scheduler::remove_pump(Pump_iterator &p)
{
  DBUG_ASSERT(p.el);

  if (m_last == p.el)
    m_last= m_last->prev;

  if (m_pumps)
  {
    m_pumps= list_delete(m_pumps, p.el);
    m_count--;
  }

  if (p)
  {
    // destructor calls driver's free() method
    delete static_cast<Pump*>(p.el->data);
    my_free(p.el, MYF(0));
  }
}

/// Shut down backup process.
void Scheduler::cancel_backup()
{
  if (cancelled)
    return;

  // shutdown any remaining drivers
  while (m_count && m_pumps)
  {
    Pump_iterator p(*this);
    p->cancel();        // Note: even if cancel() errors, we ignore it.
    remove_pump(p);     // Note: never errors.
  }

  cancelled= TRUE;
}


/// Start prepare phase for all drivers.
int Scheduler::prepare()
{
  DBUG_ASSERT(!cancelled);
  // we should start prepare phase only when init phase is finished
  DBUG_ASSERT(init_count == 0);
  DBUG_PRINT("backup_data",("calling prepare() for all drivers"));

  for (Pump_iterator it(*this); it; ++it)
  {
    if (it->prepare())
    {
      cancel_backup();  // Note: never errors.
      return ERROR;
    }
    if (it->state == backup_state::PREPARING)
     prepare_count++;
  }

  DBUG_PRINT("backup_data",("driver counts: total=%u, init=%u, prepare=%u, finish=%u.",
                            m_count, init_count, prepare_count, finish_count));
  return 0;
}

/// Lock all drivers.
int Scheduler::lock()
{
  DBUG_ASSERT(!cancelled);
  // lock only when init and prepare phases are finished
  DBUG_ASSERT(init_count == 0 && prepare_count == 0);
  DBUG_PRINT("backup_data",("calling lock() for all drivers"));

  for (Pump_iterator it(*this); it; ++it)
   if (it->lock())
   {
     cancel_backup();  // Note: never errors.
     return ERROR;
   }

  DBUG_PRINT("backup_data",("driver counts: total=%u, init=%u, prepare=%u, finish=%u.",
                            m_count, init_count, prepare_count, finish_count));
  return 0;
}

/// Unlock all drivers.
int Scheduler::unlock()
{
  DBUG_ASSERT(!cancelled);
  DBUG_PRINT("backup_data",("calling unlock() for all drivers"));

  for(Pump_iterator it(*this); it; ++it)
  {
    if (it->unlock())
    {
      cancel_backup();  // Note: never errors.
      return ERROR;
    }
    if (it->state == backup_state::FINISHING)
      finish_count++;
  }

  return 0;
}


/**************************************************

         Implementation of Backup_pump

 **************************************************/

Backup_pump::Backup_pump(Snapshot_info &snap, Block_writer &bw)
  :state(backup_state::INACTIVE), mode(READING),
  init_size(0), bytes_in(0), bytes_out(0),
  m_drv(NULL), m_bw(bw), m_buf_head(NULL),
  m_buf_retries(0)
{
  DBUG_ASSERT(snap.m_num > 0);
  m_buf.data= NULL;
  if (bitmap_init(&m_closed_streams,
              NULL,
              1 + snap.table_count(),
              FALSE)) // not thread safe
  {
    state= backup_state::ERROR;  // Created object will be invalid
    return;
  }

  m_name= snap.name();
  if (ERROR == snap.get_backup_driver(m_drv) || !m_drv)
    state= backup_state::ERROR;
  else
    init_size= m_drv->init_size();
}

Backup_pump::~Backup_pump()
{
  if (m_drv)
    m_drv->free();
  bitmap_free(&m_closed_streams);
}

/// Initialize backup driver.
int Backup_pump::begin()
{
  state= backup_state::INIT;
  DBUG_PRINT("backup_data",(" %s enters INIT state", m_name));

  if (ERROR == m_drv->begin(m_bw.buf_size))
  {
    state= backup_state::ERROR;
    // We check if logger is always setup. Later the assertion can
    // be replaced with "if (m_log)"
    DBUG_ASSERT(m_log);
      m_log->report_error(ER_BACKUP_INIT_BACKUP_DRIVER, m_name);
    return ERROR;
  }

  return 0;
}

/// Shut down the driver.
int Backup_pump::end()
{
  if (state != backup_state::SHUT_DOWN)
  {
    DBUG_PRINT("backup_data",(" shutting down %s", m_name));

    if (ERROR == m_drv->end())
    {
      state= backup_state::ERROR;
      DBUG_ASSERT(m_log);
        m_log->report_error(ER_BACKUP_STOP_BACKUP_DRIVER, m_name);
      return ERROR;
    }

    state= backup_state::SHUT_DOWN;
  }

  return 0;
}

/// Start prepare phase for the driver.
int Backup_pump::prepare()
{
  result_t res= m_drv->prelock();

  switch (res) {

  case READY:
    state= backup_state::READY;
    break;

  case OK:
    state= backup_state::PREPARING;
    break;

  case ERROR:
  default:
    state= backup_state::ERROR;
    DBUG_ASSERT(m_log);
      m_log->report_error(ER_BACKUP_PREPARE_DRIVER, m_name);
      return ERROR;
  }

  DBUG_PRINT("backup_data",(" preparing %s, goes to %s state",
                            m_name, backup_state::name[state]));
  return 0;
}

/// Request VP from the driver.
int Backup_pump::lock()
{
  DBUG_PRINT("backup_data",(" locking %s", m_name));
  if (ERROR == m_drv->lock())
  {
    state= backup_state::ERROR;
    DBUG_ASSERT(m_log);
      m_log->report_error(ER_BACKUP_CREATE_VP, m_name);
    return ERROR;
  }

  return 0;
}

/// Unlock the driver after VP creation.
int Backup_pump::unlock()
{
  DBUG_PRINT("backup_data",(" unlocking %s, goes to FINISHING state", m_name));
  state= backup_state::FINISHING;
  if (ERROR == m_drv->unlock())
  {
    state= backup_state::ERROR;
    DBUG_ASSERT(m_log);
      m_log->report_error(ER_BACKUP_UNLOCK_DRIVER, m_name);
    return ERROR;
  }

  return 0;
}

int Backup_pump::cancel()
{
  if (ERROR == m_drv->cancel())
  {
    state= backup_state::ERROR;
    DBUG_ASSERT(m_log);
      m_log->report_error(ER_BACKUP_CANCEL_BACKUP, m_name);
    return ERROR;
  }
  state= backup_state::CANCELLED;
  return 0;
}

/**
  Poll the driver for next block of data and/or write data to stream.

  Depending on the current mode in which the pump is operating (@c mode member)
  the backup driver is polled for image data or data obtained before is written
  to the stream. Answers from drivers @c get_data() method are interpreted and
  the state of the driver is updated accordingly.
*/
int Backup_pump::pump(size_t *howmuch)
{
  // pumping not allowed in these states
  DBUG_ASSERT(state != backup_state::INACTIVE);
  DBUG_ASSERT(state != backup_state::SHUT_DOWN);
  DBUG_ASSERT(state != backup_state::CANCELLED);

  // we have detected error before - report it once more
  if (state == backup_state::ERROR)
    return ERROR;

  // we are done and thus there is nothing to do
  if (state == backup_state::DONE)
    return 0;

  backup_state::value before_state= state;

  if (howmuch)
    *howmuch= 0;

  if (all_streams_closed())
  {
    switch (state) {

    case backup_state::INIT:
      state= backup_state::WAITING;
      break;

    case backup_state::PREPARING:
      state= backup_state::READY;
      break;

    case backup_state::FINISHING:
      state= backup_state::DONE;
      break;

    case backup_state::ERROR:
      return ERROR;

    default: break;
    }
  }
  else
  {
    if (mode == READING)
    {
      /*
        If m_buf_head is NULL then a new request to the driver
        should be made. We should allocate a new output buffer.
       */

      if (!m_buf_head)
        switch (m_bw.get_buf(m_buf)) {

        case Block_writer::OK:
          m_buf_retries= 0;
          m_buf_head= m_buf.data;
          break;

        case Block_writer::NO_RES:
          if (++m_buf_retries <= get_buf_retries)
            return 0; // we shall try again

        case Block_writer::ERROR:
        default:
          DBUG_ASSERT(m_log);
            m_log->report_error(ER_BACKUP_GET_BUF);
          state= backup_state::ERROR;
          return ERROR;
        }

      DBUG_ASSERT(m_buf_head);

      result_t res= m_drv->get_data(m_buf);

      switch (res) {

      case READY:

        if( state == backup_state::INIT )
          state= backup_state::WAITING;
        else if( state == backup_state::WAITING )
          state= backup_state::READY;

      case OK:

        if (m_buf.last)
        {
          mark_stream_closed(m_buf.table_num);
          if (all_streams_closed())
            DBUG_PRINT("backup_data",(" all streams of %s closed", m_name));
          else
            DBUG_PRINT("backup_data",(" stream %u closed", m_buf.table_num));
        }

        m_buf.data= m_buf_head;
        m_buf_head= NULL;

        if (m_buf.size > 0)
          mode= WRITING;
        else 
          m_bw.drop_buf(m_buf);              // Never errors

        break;

      case PROCESSING:
        break;

      case ERROR:
      default:
        DBUG_ASSERT(m_log);
          m_log->report_error(ER_BACKUP_GET_DATA, m_name);
        state= backup_state::ERROR;
        return ERROR;

      case DONE:
        state= backup_state::DONE;

      case BUSY:
        m_bw.drop_buf(m_buf);                   // Never errors
        m_buf_head=NULL;  // thus a new request will be made
      }

    } // if (mode == READING)

    if (mode == WRITING
        && state != backup_state::ERROR
        && state != backup_state::DONE)
    {
      switch (m_bw.write_buf(m_buf)) {

      case Block_writer::OK:

        if (howmuch)
          *howmuch= m_buf.size;

        DBUG_PRINT("backup_data",(" added %lu bytes from %s to archive "
                                  "(drv_num=%u, table_num=%u)",
                                  (unsigned long)howmuch, m_name, m_bw.snap_num,
                                  m_buf.table_num));
        mode= READING;
        break;

      case Block_writer::ERROR:

        DBUG_ASSERT(m_log);
          m_log->report_error(ER_BACKUP_WRITE_DATA, m_name, m_buf.table_num);
        state= backup_state::ERROR;
        return ERROR;

      default:  // retry write
        break;

      }
    }
  }

  if (state != before_state)
    DBUG_PRINT("backup_data",(" %s changes state %s->%s",
                              m_name, backup_state::name[before_state],
                                      backup_state::name[state]));
  return 0;
}

} // backup namespace


/***********************************************

                  DATA RESTORE

 ***********************************************/

namespace backup {


/**
  Read backup image data from a backup stream and forward it to restore drivers.
 */
int restore_table_data(THD *thd, Restore_info &info, Input_stream &s)
{
  DBUG_ENTER("restore::restore_table_data");

  enum { READING, SENDING, DONE, ERROR } state= READING;

  if (info.snap_count() == 0 || info.table_count() == 0) // nothing to restore
    DBUG_RETURN(0);

  Restore_driver* drv[256];

  if (info.snap_count() > 256)
  {
    info.m_ctx.fatal_error(ER_BACKUP_TOO_MANY_IMAGES, info.snap_count(), 256);
    DBUG_RETURN(ERROR);
  }

  // Create restore drivers
  result_t res;

  for (uint n=0; n < info.snap_count(); ++n)
  {
    drv[n]= NULL;

    Snapshot_info *snap= info.m_snap[n];

    // note: img can be NULL if it is not used in restore.
    if (!snap)
      continue;

    res= snap->get_restore_driver(drv[n]);
    if (res == backup::ERROR)
    {
      info.m_ctx.fatal_error(ER_BACKUP_CREATE_RESTORE_DRIVER, snap->name());
      goto error;
    };   
 }

  // Initialize the drivers.
  for (uint n=0; n < info.snap_count(); ++n)
  {
    res= drv[n]->begin(0);
    if (res == backup::ERROR)
    {
      info.m_ctx.fatal_error(ER_BACKUP_INIT_RESTORE_DRIVER, info.m_snap[n]->name());
      goto error;
    }
  }

  DEBUG_SYNC(thd, "restore_in_progress");
  {
    Buffer  buf;
    uint    snap_num=0;
    uint    repeats=0, errors= 0;
    int     ret;

    static const uint MAX_ERRORS= 3;
    static const uint MAX_REPEATS= 7;

    Restore_driver  *drvr= NULL;  // pointer to the current driver
    Snapshot_info   *snap= NULL;   // corresponding snapshot object

    // main data reading loop

    st_bstream_data_chunk chunk_info;

    while ( state != DONE && state != ERROR )
    {
      switch (state) {

      case READING:

        bzero(&chunk_info, sizeof(chunk_info));
        ret= bstream_rd_data_chunk(&s, &chunk_info);

        switch (ret) {

        case BSTREAM_EOS:
        case BSTREAM_EOC:
          state= DONE;
          break;

        case BSTREAM_OK:
          state= SENDING;
          break;

        case BSTREAM_ERROR:
          info.m_ctx.fatal_error(ER_BACKUP_READ_DATA);
        default:
          state= ERROR;
          goto error;

        }

        if (state != SENDING)
          break;

        // data chunk should never be empty
        DBUG_ASSERT(chunk_info.data.begin);
        DBUG_ASSERT(chunk_info.data.begin < chunk_info.data.end);

        snap_num= chunk_info.snap_num;
        buf.table_num= chunk_info.table_num;
        buf.last= chunk_info.flags & BSTREAM_FLAG_LAST_CHUNK;
        buf.data= chunk_info.data.begin;
        buf.size= chunk_info.data.end - chunk_info.data.begin;

        if (snap_num > info.snap_count() || !(drvr= drv[snap_num]))
        {
          DBUG_PRINT("restore",("Skipping data from snapshot #%u", snap_num));
          state= READING;
          break;
        }

        snap= info.m_snap[snap_num];
        // Each restore driver should have corresponding Image_info object.
        DBUG_ASSERT(snap);

        DBUG_PRINT("restore",("Got %lu bytes of %s image data (for table #%u)",
                   (unsigned long)buf.size, snap->name(), buf.table_num));

      case SENDING:

        /*
          If we are here, the img pointer should point at the image for which
          we have next data block and drvr at its restore driver.
         */
        DBUG_ASSERT(snap && drvr);

        switch( drvr->send_data(buf) ) {

        case backup::OK:
          info.data_size += buf.size;
          state= READING;
          snap= NULL;
          drvr= NULL;
          repeats= 0;
          break;

        case backup::ERROR:
          if( errors > MAX_ERRORS )
          {
            info.m_ctx.fatal_error(ER_BACKUP_SEND_DATA, buf.table_num, snap->name());
            state= ERROR;
            goto error;
          }
          errors++;
          break;

        case backup::PROCESSING:
        case backup::BUSY:
        default:
          if( repeats > MAX_REPEATS )
          {
            info.m_ctx.fatal_error(ER_BACKUP_SEND_DATA_RETRY, repeats, snap->name());
            state= ERROR;
            goto error;
          }
          repeats++;
          break;
        }

      default:
        break;
      } // switch(state)

    } // main reading loop

    DBUG_PRINT("restore",("End of backup stream"));
    if (state != DONE)
      DBUG_PRINT("restore",("state is %d", state));
  }

  DEBUG_SYNC(::current_thd, "restore_table_data_before_end");
  
  { // Shutting down drivers

    String bad_drivers;

    for (uint n=0; n < info.snap_count(); ++n)
    {
      if (!drv[n])
        continue;

      DBUG_PRINT("restore",("Shutting down restore driver %s",
                            info.m_snap[n]->name()));
      res= drv[n]->end();
      if (res == backup::ERROR)
      {
        state= ERROR;

        if (!bad_drivers.is_empty())
          bad_drivers.append(",");
        bad_drivers.append(info.m_snap[n]->name());
      }
      drv[n]->free();                           // Never errors
    }

    if (!bad_drivers.is_empty())
      info.m_ctx.report_error(ER_BACKUP_STOP_RESTORE_DRIVERS, bad_drivers.c_ptr());
  }

  DBUG_RETURN(state == ERROR ? backup::ERROR : 0);

 error:

  DBUG_PRINT("restore",("Cancelling restore process"));

  for (uint n=0; n < info.snap_count(); ++n)
  {
    if (!drv[n])
      continue;

    drv[n]->free();                             // Never errors
  }

  DBUG_RETURN(backup::ERROR);
}


} // backup namespace


/**************************************************

       Implementation of Block_writer

 **************************************************/

namespace backup {

Block_writer::Block_writer(byte snap_num, size_t size, Output_stream &s)
  :snap_num(snap_num), m_str(s), buf_size(size), taken(FALSE)
{
  data_buf= (byte*)my_malloc(buf_size, MYF(0));
}

Block_writer::~Block_writer()
{
  if (data_buf)
    my_free(data_buf, MYF(0));
  data_buf= NULL;
}

/**
  Allocate new buffer for data transfer.

  The buffer size is given by @c buf.size member.

  Current implementation tries to allocate the data transfer buffer in the
  stream. It can handle only one buffer at a time.

  @returns @c NO_RES if buffer can not be allocated, @c OK otherwise.
 */
Block_writer::result_t
Block_writer::get_buf(Buffer &buf)
{
  buf.table_num= 0;
  buf.last= FALSE;
  buf.size= buf_size;
  buf.data= NULL;

  if (taken)
    return NO_RES;

  buf.data= data_buf;

  if (!buf.data)
    return NO_RES;

  taken= TRUE;

  return OK;
}

/**
  Write block of data to stream.

  The buffer containing data must be obtained from a previous @c get_buf() call.
  After this call, buffer is returned to the buffer pool and can be reused for
  other transfers.
 */
Block_writer::result_t
Block_writer::write_buf(const Buffer &buf)
{
  st_bstream_data_chunk  chunk_info;

  chunk_info.table_num= buf.table_num;
  chunk_info.data.begin= buf.data;
  chunk_info.data.end= buf.data + buf.size;
  chunk_info.flags= buf.last ? BSTREAM_FLAG_LAST_CHUNK : 0x00;
  chunk_info.snap_num= snap_num;

  int ret= bstream_wr_data_chunk(&m_str, &chunk_info);

  if (ret == BSTREAM_OK)
  {
    taken= FALSE;
    return OK;
  }
  else
    return ERROR;
}

/**
  Return buffer to the buffer pool.

  If a buffer obtained from @c get_buf() is not written to the stream, this
  method can return it to the buffer pool so that it can be reused for other
  transfers.
 */
void
Block_writer::drop_buf(Buffer &buf)
{
  buf.data= NULL;
  buf.size= 0;
  taken= FALSE;
}

} // backup namespace
