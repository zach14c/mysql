/* Copyright (C) 2009 - 2009 Sun Microsystems, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

/**
  @file
  Logging of Maria commands and records, unrelated to transaction log.

  The logs implemented here have NOTHING to do with the transaction log which
  contains REDOs and UNDOs and is used for Recovery and ROLLBACK.

  The physical log contains each call to OS write functions on the Maria
  files. Most of its entries are physical for example "write these bytes at
  this offset". For example, a maria_write() with lots of BLOBs in many places
  will cause lots of entries in this log. It also contains some logical ones
  like MA_LOG_DELETE_ALL (we wouldn't want to log the deletion of all rows
  one by one).

  In MyISAM there is a logical log (contains each call to higher-level
  operations like mi_write()/mi_update()) but not in Maria.

  Writes to the physical log happen when the physical operation happens,
  i.e. when the file is written, which can be at three moments:
  -# when the row write directly writes to the file (_ma_[no]mmap_pwrite())
  -# if the row write went to a WRITE_CACHE, when this cache gets written to
     the file (post_write callback in that cache)
  -# if the row write went to the page cache, when this cache block gets
     written ("flushed") to the file (post_write callback in that cache)
  Additionally, an entry for opening and an entry for closing the table, are
  written to the physical log: the first "direct row write" or "WRITE_CACHE"
  or "page cache block flush" log write for a certain MARIA_SHARE, an entry
  for opening (MA_LOG_OPEN) is written. All entries refer to the table by the
  file descriptor of the index file; the MA_LOG_OPEN entry links this number
  to a table name. The entry for closing is written by maria_close() if an
  entry for opening had been written before and if the index file is being
  closed.

  Physical log is used for online backup, because if applied to a dirtily
  copied table it can make this table consistent.

  This log:
  - is idempotent (if you apply such log to a table, then applying it a
  second time has no effect).
  - can be used to debug Maria
  - can be examined and applied to tables with the maria_non_trans_log
  utility.

  Physical log is about to a set of tables, can be turned on and off at any
  time.

  ma_log() is the entry point.
*/

#include "maria_def.h"
#if defined(MSDOS) || defined(__WIN__)
#include <fcntl.h>
#ifndef __WIN__
#include <process.h>
#endif
#endif
#ifdef VMS
#include <processes.h>
#endif

#undef GETPID					/* For HPUX */
#ifdef THREAD
#define GETPID() (log_type == 1 ? (long) maria_pid : (long) my_thread_dbug_id())
#else
#define GETPID() maria_pid
#endif

/** the log_type global variable is probably obsolete, it's always 0 now */
static const int log_type=0;
ulong maria_pid=0;
static int ma_log_open_cache(const char *log_filename);
static int ma_log_close_cache();
static int ma_log_start_physical(const char *log_filename,
                                 const HASH *tables);
static int ma_log_stop_physical();


/**
  Starts Maria physical logging for a set of tables, or stops it.

  @param  action           what to do (start, stop (in)consistently)
  @param  log_filename     name of the log file to create
  @param  tables           hash of names of tables for which we want logging
                           (only for physical log)

  @return Operation status
    @retval 0      ok
    @retval !=0    error; then caller should call ma_log_stop_physical(TRUE)
*/

int ma_log(enum enum_ma_log_action action,
           const char *log_filename, const HASH *tables)
{
  int error;
  DBUG_ENTER("ma_log");

#ifndef HAVE_MARIA_PHYSICAL_LOGGING
  DBUG_ASSERT(0);
  DBUG_RETURN(1);
#endif

  /* starting/stopping are complex operations so split in functions */
  switch (action)
  {
  case MA_LOG_ACTION_OPEN:
    error= ma_log_start_physical(log_filename, tables);
    break;
  case MA_LOG_ACTION_CLOSE_CONSISTENT:
  case MA_LOG_ACTION_CLOSE_INCONSISTENT:
    error= ma_log_stop_physical(action);
    break;
  default:
    DBUG_ASSERT(0);
    error= 1;
  }
  DBUG_RETURN(error);
}


/**
  Sets up a log's IO_CACHE (for physical log).

  Log is IO_CACHE to be fast.

  @param  log_filename     name of file to create

  @note logs are not created with MY_WAIT_IF_FULL: a log can itself be the
  cause of filling the disk, so better corrupt it (and make a backup
  fail for example) than prevent other normal operations.

  @todo A realistic benchmark to see if the size of the IO_CACHE makes any
  speed difference.

  @return Operation status
    @retval 0      ok
    @retval !=0    error
*/

static int ma_log_open_cache(const char *log_filename)
{
  int error=0;
  char buff[FN_REFLEN];
  int access_flags;
  File file;
  IO_CACHE *log;
  uint cache_size;
  DBUG_ENTER("ma_log_open_cache");

  DBUG_ASSERT(log_filename != NULL);
  pthread_mutex_lock(&THR_LOCK_maria_log);
  log= &maria_physical_log;
  /* We want to fail if file exists */
  access_flags= O_WRONLY | O_BINARY | O_TRUNC | O_EXCL;
  /*
    We want a large IO_CACHE to have large contiguous disk writes.
    In many systems this size is affordable. In small embedded ones it is
    not, but would they use this log?
  */
  cache_size= IO_SIZE*256;

  if (!maria_pid)
    maria_pid=(ulong) getpid();
  if (!my_b_inited(log))
  {
    DBUG_ASSERT(log_filename);
    fn_format(buff, log_filename, "", "", MY_UNPACK_FILENAME);
    if ((file= my_create(buff,
                         0, access_flags,
                         MYF(MY_WME | ME_WAITTANG))) < 0)
      error= my_errno;
    else if (init_io_cache(log, file,
                           cache_size, WRITE_CACHE,
                           my_tell(file,MYF(MY_WME)), 0,
                           MYF(MY_WME | MY_NABP)))
    {
      error= my_errno;
      my_close(file, MYF(MY_WME));
    }
  }
  pthread_mutex_unlock(&THR_LOCK_maria_log);
  DBUG_RETURN(error);
}


/**
  Destroy's a log's IO_CACHE for physical log

  @return Operation status
    @retval 0      ok
    @retval !=0    error
*/

static int ma_log_close_cache()
{
  int error= 0;
  IO_CACHE *log;
  DBUG_ENTER("ma_log_close_cache");
  pthread_mutex_lock(&THR_LOCK_maria_log);
  log         = &maria_physical_log;
  if (my_b_inited(log))
  {
    if (end_io_cache(log) ||
        my_close(log->file,MYF(MY_WME)))
      error= my_errno;
    log->file= -1;
  }
  pthread_mutex_unlock(&THR_LOCK_maria_log);
  DBUG_RETURN(error);
}


/**
  Logs a Maria command and its return code to log.

  If MA_LOG_OPEN has not already been stored for this MARIA_SHARE in this log,
  also writes a MA_LOG_OPEN.

  @param  log              pointer to the log's IO_CACHE
  @param  command          Maria command (see code for allowed commands)
  @param  share            MARIA_SHARE
  @param  buffert          usually argument to the command (e.g. name of file
                           to open for MA_LOG_OPEN), may be NULL
  @param  length           length of buffert (0 if NULL)
  @param  result           return code of the command
*/

void _maria_log_command(IO_CACHE *log, enum maria_log_commands command,
                         MARIA_SHARE *share,
                         const uchar *buffert, uint length, int result)
{
  uchar header[14];
  int old_errno, headerlen;
  ulong pid=(ulong) GETPID();
  File file;

  file= share->kfile.file;
  LINT_INIT(error);

  DBUG_ASSERT(command == MA_LOG_OPEN  || command == MA_LOG_CLOSE ||
              command == MA_LOG_DELETE_ALL);
  old_errno=my_errno;
  DBUG_ASSERT(((uint)result) <= UINT_MAX16);
  if (file >= UINT_MAX16 || length >= UINT_MAX16)
  {
    header[0]= ((uchar) command) | MA_LOG_BIG_NUMBERS;
    DBUG_ASSERT(file < (2<<24));
    mi_int3store(header + 1, file);
    mi_int4store(header + 4, pid);
    mi_int2store(header + 8, result);
    mi_int4store(header + 10, length);
    headerlen= 14;
  }
  else
  {
    /* use a compact encoding for all these small numbers */
    header[0]= (uchar) command;
    mi_int2store(header + 1, file);
    mi_int4store(header + 3, pid);
    mi_int2store(header + 7, result);
    mi_int2store(header + 9, length);
    headerlen= 11;
  }
retry:
  /*
    Reasons to not use THR_LOCK_maria to serialize log writes:
    - better concurrency (not stealing THR_LOCK_maria which is used for opens
    and closes including long table flushes)
    - maria_close() flushes indexes while holding THR_LOCK_maria, and that
    flush can cause log writes, so we would lock the mutex twice.
  */
  pthread_mutex_lock(&THR_LOCK_maria_log);
  /*
    We need to check that 'log' is not closed, this can happen for a physical
    log. Indeed we do not have full control on the table from the thread doing
    ma_log_stop_physical(); it could be an inconsistent logging stop (in
    the middle of writes) or even a consistent one (table can be in
    maria_lock_database(F_UNLCK) and thus want to flush its header)). Log
    might just have been closed while the table still has physical_logging
    true.
  */
  if (likely(my_b_inited(log) != NULL))
  {
    if (command == MA_LOG_OPEN)
    {
      /*
        If there could be two concurrent writers on a Maria table, it could
        be that they both do a maria_log_command(c) where c!=MA_LOG_OPEN,
        which both see MA_LOG_OPEN_stored_in_physical_log false, and both
        call maria_log_command(MA_LOG_OPEN); we would then have to make one
        single winner: one will run before the other, the other should
        notice MA_LOG_OPEN_stored_in_physical_log became true and back off.
        But there is always at most one writer to a Maria table, so the
        assertion below should always be ok
      */
      DBUG_ASSERT(!share->MA_LOG_OPEN_stored_in_physical_log);
      share->MA_LOG_OPEN_stored_in_physical_log= TRUE;
      /*
        We must keep the mutex between setting the boolean above and writing
        to the log ; one instant after unlocking the mutex, the log may be
        closed and so it would be wrong to say that the MA_LOG_OPEN is in
        the log (it would possibly influence a next physical log).
      */
    }
    else if (unlikely(!share->MA_LOG_OPEN_stored_in_physical_log))
    {
      DBUG_ASSERT(command != MA_LOG_CLOSE);
      pthread_mutex_unlock(&THR_LOCK_maria_log);
      _maria_log_command(&maria_physical_log, MA_LOG_OPEN, share,
                         share->open_file_name.str,
                         share->open_file_name.length, 0);
      goto retry;
    }
    /*
      Any failure to write the log does not prevent the table write (table
      should still be usable even though log breaks).
      but sets up log->hard_write_error_in_the_past, which can be tested by
      those who want to use this log.
    */
    (void) my_b_write(log, header, headerlen);
    if (buffert)
      (void) my_b_write(log, buffert, length);
    else
    {
      DBUG_ASSERT(length == 0);
    }
  }
  pthread_mutex_unlock(&THR_LOCK_maria_log);
  my_errno=old_errno;
}


/**
  Logs a my_pwrite() (done to data or index file) to the physical log.

  Also logs MA_LOG_OPEN if first time. Thus, a MARIA_HA will write MA_LOG_OPEN
  to the log only if it is doing a write to the table: a table which does
  only reads logs nothing to the physical log.

  @param  command          Maria command (MA_LOG_WRITE_BYTES_TO_MAD|MAI)
  @param  share            table's share
  @param  buffert          argument to the pwrite
  @param  length           length of buffer
  @param  filepos          offset in file where buffer was written

  @note length may be small (for example, if updating only a numeric field of
  a record, it could be only a few bytes), so we try to minimize the header's
  size of the log entry (no 'pid', no 'result').
*/

void maria_log_pwrite_physical(enum maria_log_commands command,
                                MARIA_SHARE *share, const uchar *buffert,
                                uint length, my_off_t filepos)
{
  uchar header[21];
  int old_errno, headerlen;
  DBUG_ENTER("maria_log_pwrite_physical");
  DBUG_ASSERT(command == MA_LOG_WRITE_BYTES_MAD ||
              command == MA_LOG_WRITE_BYTES_MAI);
  DBUG_ASSERT(buffert != NULL && length > 0);
  old_errno= my_errno;
  if (share->kfile.file >= UINT_MAX16 || filepos >= UINT_MAX32 ||
      length >= UINT_MAX16)
  {
    header[0]= ((uchar) command) | MA_LOG_BIG_NUMBERS;
    DBUG_ASSERT(share->kfile.file < (2<<24));
    mi_int3store(header + 1, share->kfile.file);
    mi_sizestore(header + 4, filepos);
    mi_int4store(header + 12, length);
    headerlen= 16;
  }
  else
  {
    header[0]= (uchar) command;
    mi_int2store(header + 1, share->kfile.file);
    mi_int4store(header + 3, filepos);
    mi_int2store(header + 7, length);
    headerlen= 9;
  }
  /* pid and result are not needed */
retry:
  pthread_mutex_lock(&THR_LOCK_maria_log);
  if (likely(my_b_inited(&maria_physical_log) != NULL))
  {
    if (unlikely(!share->MA_LOG_OPEN_stored_in_physical_log))
    {
      pthread_mutex_unlock(&THR_LOCK_maria_log);
      _maria_log_command(&maria_physical_log, MA_LOG_OPEN, share,
                          share->open_file_name.str,
                          share->open_file_name.length, 0);
      goto retry;
    }
    (void) my_b_write(&maria_physical_log, header, headerlen);
    (void) my_b_write(&maria_physical_log, buffert, length);
  }
  pthread_mutex_unlock(&THR_LOCK_maria_log);
  my_errno= old_errno;
  DBUG_VOID_RETURN;
}


/**
  Logs a my_chsize() done to the index file to the physical log.

  Also logs MA_LOG_OPEN if first time.

  @param  share            table's share
  @param  new_length       new length of the table's index file
*/

void maria_log_chsize_kfile_physical(MARIA_SHARE *share,
                                      my_off_t new_length)
{
  uchar header[12];
  int old_errno, headerlen;
  DBUG_ENTER("maria_log_chsize_kfile_physical");
  old_errno= my_errno;
  if (share->kfile.file >= UINT_MAX16 || new_length >= UINT_MAX32)
  {
    header[0]= MA_LOG_CHSIZE_MAI | MA_LOG_BIG_NUMBERS;
    DBUG_ASSERT(share->kfile.file < (2<<24));
    mi_int3store(header + 1, share->kfile.file);
    mi_sizestore(header + 4, new_length);
    headerlen= 12;
  }
  else
  {
    header[0]= MA_LOG_CHSIZE_MAI;
    mi_int2store(header + 1, share->kfile.file);
    mi_int4store(header + 3, new_length);
    headerlen= 7;
  }
  /* pid and result are not needed */
retry:
  pthread_mutex_lock(&THR_LOCK_maria_log);
  if (likely(my_b_inited(&maria_physical_log) != NULL))
  {
    if (unlikely(!share->MA_LOG_OPEN_stored_in_physical_log))
    {
      pthread_mutex_unlock(&THR_LOCK_maria_log);
      _maria_log_command(&maria_physical_log, MA_LOG_OPEN, share,
                          share->open_file_name.str,
                          share->open_file_name.length, 0);
      goto retry;
    }
    (void) my_b_write(&maria_physical_log, header, headerlen);
  }
  pthread_mutex_unlock(&THR_LOCK_maria_log);
  my_errno= old_errno;
  DBUG_VOID_RETURN;
}


/**
  Starts Maria physical logging for a set of tables.

  Physical logging is used for online backup.
  A condition of correctness of online backup is that:
  after the copy process has started (i.e. after the function below has
  terminated), any update done to a table-to-back-up must be present in the
  log. This guides the algorithm below.

  All writes (my_write, my_pwrite, memcpy to mmap'ed area, my_chsize) to the
  data or index file are done this way:
  @code
  {
    write_to_data_or_index_file;
    if ((atomic read of MARIA_SHARE::physical_logging) != 0)
      write log record to physical log;
  }
  @endcode

  The present function sets MARIA_SHARE::physical_logging to 1 using an
  atomic write. Atomic write happens before or after atomic read above, and
  atomic read sees the latest value. If before, change will be in the log. If
  after, it is also after the write_to_data_or_index_file and thus change
  will be in the copy. So correctness is always guaranteed. Note the
  importance of checking MARIA_SHARE::logging always _after_
  write_to_data_or_index_file, with an _atomic_read_ for the reasoning to
  hold.

  @param  log_filename     Name of the physical log file to create
  @param  tables           Hash of names of tables for which we want logging

  @return Operation status
    @retval 0      ok
    @retval !=0    error
*/

static int ma_log_start_physical(const char *log_filename, const HASH *tables)
{
  LIST *list_item;
  int error;
  DBUG_ENTER("ma_log_start_physical");
  DBUG_ASSERT(log_filename != NULL);
  DBUG_ASSERT(hash_inited(tables));

  pthread_mutex_lock(&THR_LOCK_maria);
  if (ma_log_tables_physical) /* physical logging already running */
  {
    pthread_mutex_unlock(&THR_LOCK_maria);
    DBUG_ASSERT(0); /* because it should not happen */
    DBUG_RETURN(1);
  }
  ma_log_tables_physical= tables;

  if (unlikely(ma_log_open_cache(log_filename)))
  {
    error= 1;
    goto end;
  }
  /* Go through all open Maria tables */
  for (list_item= maria_open_list; list_item; list_item= list_item->next)
  {
    MARIA_HA *info= (MARIA_HA*)list_item->data;
    MARIA_SHARE *share= info->s;
    DBUG_PRINT("info",("table '%s' 0x%lx tested against hash",
                       share->unique_file_name.str, (ulong)info));
    if (!hash_search(ma_log_tables_physical,
                     share->unique_file_name.str,
                     share->unique_file_name.length))
      continue;
    /* Backup kernel shouldn't ask for temporary table's backup */
    DBUG_ASSERT(!share->temporary);
    /*
      We don't need to flush key blocks, WRITE_CACHE or the state
      because every time they are written to disk (at the latest in
      ma_log_stop_physical()) they check for physical logging
      (key cache always has log_key_page_flush_physical() as
      post_write, WRITE_CACHE always has log_flushed_write_cache_physical()
      has post_write, even when _not_ in backup), so any now cached info will
      finally reach the log.
      Conversely, if we wanted to register no callback in key cache and
      WRITE_CACHE when no backup is running (to save function calls
      and atomic reads when no backup is running), we would have to
      flush key cache and WRITE_CACHE here.
    */
    ma_set_physical_logging_state(info->s, 1);
  }
  error= 0;
end:
  pthread_mutex_unlock(&THR_LOCK_maria);
  if (unlikely(error))
    ma_log_stop_physical(MA_LOG_ACTION_CLOSE_INCONSISTENT, NULL, NULL);
  DBUG_RETURN(error);
}


/**
  Stops Maria physical logging.

  As part of this stop operation, user can request that the physical log ends
  in a consistent state, i.e. that it contains copies of the currently cached
  key pages etc. To be consistent assumes that the caller has relevant tables
  write-locked, indeed otherwise the log could end in the middle of a
  statement, and applying it would produce a likely corrupted table.Online
  backup needs such a consistent log to be able to create consistent table
  copies from the log. If online backup is being cancelled, then there is no
  need that the physical log be consistent.

  @param  action           MA_LOG_ACTION_CLOSE_CONSISTENT or
                           MA_LOG_ACTION_CLOSE_INCONSISTENT.

  @return Operation status
    @retval 0      ok
    @retval !=0    error

  @note Even if MA_LOG_ACTION_CLOSE_CONSISTENT, tables may be being written
  now (in practice caller has read-locked tables, but those tables may be
  just going out of a write (after thr_unlock(), before or inside
  maria_lock_database(F_UNLCK) which may be flushing the index header or index
  pages).
*/

static int ma_log_stop_physical(enum enum_ma_log_action action)
{
  int error= 0;
  LIST *list_item;
  DBUG_ENTER("ma_log_stop_physical");
  DBUG_ASSERT(action == MA_LOG_ACTION_CLOSE_CONSISTENT ||
              action == MA_LOG_ACTION_CLOSE_INCONSISTENT);

  pthread_mutex_lock(&THR_LOCK_maria);
  if (ma_log_tables_physical == NULL) /* no physical logging running */
  {
    pthread_mutex_unlock(&THR_LOCK_maria);
    DBUG_RETURN(0); /* it's ok if it happens */
  }
  /*
    This is a pointer to a object provided by the caller through
    ma_log_start_physical(); such object is to be freed by the caller.
  */
  ma_log_tables_physical= NULL;

  if (action == MA_LOG_ACTION_CLOSE_CONSISTENT)
  {
    /**
      @todo consider an algorithm which would not keep THR_LOCK_maria for the
      time of flushing all these tables' indices; we could do a first loop
      with THR_LOCK_maria to collect shares and "pin" them; then a second
      loop without THR_LOCK_maria, flushing and unpinning them.
    */
    for (list_item= maria_open_list; list_item; list_item= list_item->next)
    {
      MARIA_HA *info= (MARIA_HA*)list_item->data;
      MARIA_SHARE *share= info->s;
      /*
        Setting of the variable below always happens under THR_LOCK_maria,
        which we have here, so we don't need atomic operations to read here.
      */
      if (!share->physical_logging)
        continue;
      /*
        Must take intern_lock, at least because key cache isn't safe if two
        calls to flush_key_blocks_int() run concurrently on the same file.
      */
      pthread_mutex_lock(&share->close_lock);
      pthread_mutex_lock(&share->intern_lock);
      /*
        It is possible that some statement just finished, has not called
        maria_lock_database(F_UNLCK) yet, and so some data/index blocks would
        still be in memory. So we have to flush below, to put them into the
        log.

        It is also possible (same scenario) that some WRITE_CACHE is not
        flushed yet. This should not happen but it does (can just be a
        forgotten maria_extra(HA_EXTRA_NO_CACHE)); so maria_close() and
        maria_lock_database(F_UNLCK) flush the cache; so we have to do it here
        too, to put the data into the log. Mutices in maria_close() and
        maria_lock_database() ensure that they don't flush at the same time as
        us (which could corrupt the cache). Nobody should flush the
        WRITE_CACHE without a write-lock or intern_lock (see assertion in
        maria_reset()).

        It is also possible (same scenario) that the index's header has not
        been written yet and nobody is going to do it for us; indeed this can
        happen (two concurrent threads): thread1 has just done
        maria_lock_database(F_WRLCK), is blocked by the thr_lock of our caller,
        thread2 has finished its write statement and is going to execute
        maria_lock_database(F_UNLCK); no index header flush will be done by the
        maria_lock_database(F_UNLCK) of thread2 as w_locks is >0 (due to
        thread1). And no index header flush will be done by thread1 as it is
        blocked. So, we need to flush the index header here, to put it into
        the log.

        Of course, for the flushing above to reach the log, it has to be done
        before setting share->physical_logging to false and before closing the
        log.
      */
      if (_ma_flush_table_files(info, (((info->dfile.file >= 0)) ?
                                       MARIA_FLUSH_DATA : 0) | 
                                ((ma_log_index_pages_physical &&
                                  (share->kfile.file >= 0)) ?
                                 MARIA_FLUSH_INDEX : 0),
                                FLUSH_KEEP, FLUSH_KEEP) ||
          ((info->opt_flag & WRITE_CACHE_USED) &&
           flush_io_cache(&info->rec_cache)) ||
          (share->changed &&
           ma_remap_file_and_write_state_for_unlock(info, TRUE)))
      {
        error= 1; /* we continue, because log has to be closed anyway */
        maria_print_error(share, HA_ERR_CRASHED);
        maria_mark_crashed(info);	/* Mark that table must be checked */
      }
      pthread_mutex_unlock(&share->intern_lock);
      pthread_mutex_unlock(&share->close_lock);
    } /* ... for (list_item=...) */
  } /* ... if (action == MA_LOG_ACTION_CLOSE_CONSISTENT) */

  /*
    Online backup wants to pick this log with my_read() calls, to send it to
    the backup stream. So we don't delete log but close it now, so that its
    IO_CACHE goes to disk (so that all log is visible to the my_read()
    calls). Another reason related to concurrency is mentioned below.
  */
  if (ma_log_close_cache())
    error= 1;

  for (list_item= maria_open_list; list_item; list_item= list_item->next)
  {
    MARIA_SHARE *share= ((MARIA_HA*)list_item->data)->s;
    /*
      Setting of the variable below always happens under THR_LOCK_maria,
      which we have here, so we don't need atomic operations to read here.
    */
    if (!share->physical_logging)
      continue;
    ma_set_physical_logging_state(share, 0);
    /*
      We reset MA_LOG_OPEN_stored_in_physical_log. How is this safe with a
      concurrent logging operation (like maria_log_pwrite_physical()) which
      may want to set it to TRUE at the same time?
      The concurrent logging operation runs either before or after log closing
      (serialization ensured by THR_LOCK_maria_log). If before, it is before
      us (us==resetter), because log closing is before us, so we win. If
      after, the concurrent logging operation finds the log closed and so
      will not change MA_LOG_OPEN_stored_in_physical_log (so we win again).
      Note the importance of closing the log before, for the reasoning to
      hold.
    */
    share->MA_LOG_OPEN_stored_in_physical_log= FALSE;
  }

  pthread_mutex_unlock(&THR_LOCK_maria);
  /*
    From this moment on, from the point of view of Maria, a new physical log
    (a new backup) can start (new log will use a different tmp name).
  */
  DBUG_RETURN(error);
}
