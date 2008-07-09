/* Copyright (C) 2000-2001, 2004 MySQL AB

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
  Logging of MyISAM commands and records.

  The physical log contains each call to OS write functions on the MyISAM
  files. Most of its entries are physical for example "write these bytes at
  this offset". For example, a mi_write() with lots of BLOBs in many places
  will cause lots of entries in this log. It also contains some logical ones
  like MI_LOG_DELETE_ALL (we wouldn't want to log the deletion of all rows
  one by one).

  The logical log contains each call to higher-level operations like
  mi_write()/mi_update(). A sample entry is "write this record". For example,
  a mi_write() with lots of BLOBs will cause one single entry in this log.

  Writes to the logical log happen when the logical operation
  happens. Entries in it refer to a table by file descriptor of its data
  file.

  Writes to the physical log happen when the physical operation happens,
  i.e. when the file is written, which can be at three moments:
  -# when the row write directly writes to the file (mi_[no]mmap_pwrite())
  -# if the row write went to a WRITE_CACHE, when this cache gets written to
     the file (post_write callback in that cache)
  -# if the row write went to the key cache, when this key cache block gets
     written ("flushed") to the file (post_write callback in that cache)
  Additionally, an entry for opening and an entry for closing the table, are
  written to the physical log: the first "direct row write" or "WRITE_CACHE"
  or "key cache block flush" log write for a certain MYISAM_SHARE, an entry
  for opening (MI_LOG_OPEN) is written. All entries refer to the table by the
  file descriptor of the index file; the MI_LOG_OPEN entry links this number
  to a table name. The entry for closing is written by mi_close() if an entry
  for opening had been written before and if the index file is being closed.

  Physical log is used for online backup, because if applied to a dirtily
  copied table it can make this table consistent. A dirty table
  contains internally inconsistent records, so applying a logical log to it
  (like mi_update()) is impossible).

  Both logs:
  - are idempotent (if you apply such log to a table, then applying it a
  second time has no effect).
  - can be used to debug MyISAM
  - can be examined and applied to tables with the myisamlog utility.

  Logical log is about all tables, is turned on before opening tables (for
  example turned on at program's start and off at program's end).

  Physical log is about to a set of tables, can be turned on and off at any
  time.

  mi_log() is the entry point.
*/

#include "myisamdef.h"
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
#define GETPID() (log_type == 1 ? (long) myisam_pid : (long) my_thread_dbug_id())
#else
#define GETPID() myisam_pid
#endif

/** the log_type global variable is probably obsolete, it's always 0 now */
static const int log_type=0;
ulong myisam_pid=0;
static int mi_log_open_cache(enum enum_mi_log_type type,
                             const char *log_filename);
static int mi_log_close_cache(enum enum_mi_log_type type);
static int mi_log_start_physical(const char *log_filename,
                                 const HASH *tables);
static int mi_log_stop_physical();


/**
  Starts MyISAM logical logging for all tables or physical logging for
  a set of tables, or stops it.

  @param  action           what to do (start, stop (in)consistently)
  @param  type             physical or logical
  @param  log_filename     name of the log file to create (only for physical
                           log, logical log has a static name)
  @param  tables           hash of names of tables for which we want logging
                           (only for physical log)

  @note For the logical log, MI_LOG_CLOSE_INCONSISTENT and
  MI_LOG_CLOSE_CONSISTENT are identical: log will be consistent only if
  tables not being written now.

  @return Operation status
    @retval 0      ok
    @retval !=0    error; then caller should call mi_log_stop_physical(TRUE)
*/

int mi_log(enum enum_mi_log_action action, enum enum_mi_log_type type,
           const char *log_filename, const HASH *tables)
{
  int error;
  DBUG_ENTER("mi_log");

  if (type == MI_LOG_LOGICAL)
  {
    DBUG_ASSERT(log_filename == NULL);
    DBUG_ASSERT(tables == NULL);
    if (action == MI_LOG_ACTION_OPEN)
      error= mi_log_open_cache(type, myisam_logical_log_filename);
    else
      error= mi_log_close_cache(type);
    DBUG_RETURN(error);
  }

#ifndef HAVE_MYISAM_PHYSICAL_LOGGING
  DBUG_ASSERT(0);
  DBUG_RETURN(1);
#endif

  /* starting/stopping are complex operations so split in functions */
  switch (action)
  {
  case MI_LOG_ACTION_OPEN:
    error= mi_log_start_physical(log_filename, tables);
    break;
  case MI_LOG_ACTION_CLOSE_CONSISTENT:
  case MI_LOG_ACTION_CLOSE_INCONSISTENT:
    error= mi_log_stop_physical(action);
    break;
  default:
    DBUG_ASSERT(0);
    error= 1;
  }
  DBUG_RETURN(error);
}


/**
  Sets up a log's IO_CACHE (for logical or physical log).

  Both logs are IO_CACHE to be fast. But the logical log flushes it after
  every operation, as it is share-able between different processes and so
  another process may want to write to this log as soon as we unlock its
  file. Also, as this log is used for debugging it must contain as much
  information as possible in case of crash. For this log, using IO_CACHE
  still makes sense as it decreases the number of my_write() calls.

  @param  type             physical or logical
  @param  log_filename     only for physical log (logical log has a static
                           name)

  @note logs are not created with MY_WAIT_IF_FULL: a log can itself be the
  cause of filling the disk, so better corrupt it (and make a backup
  fail for example) than prevent other normal operations.

  @todo A realistic benchmark to see if the size of the IO_CACHE makes any
  speed difference.

  @return Operation status
    @retval 0      ok
    @retval !=0    error
*/

static int mi_log_open_cache(enum enum_mi_log_type type,
                             const char *log_filename)
{
  int error=0;
  char buff[FN_REFLEN];
  int access_flags;
  File file;
  IO_CACHE *log;
  uint cache_size;
  DBUG_ENTER("mi_log_open_cache");

  DBUG_ASSERT(log_filename != NULL);
  pthread_mutex_lock(&THR_LOCK_myisam_log);
  if (type == MI_LOG_LOGICAL)
  {
    log= &myisam_logical_log;
    /* O_APPEND as file may exist and we want to keep it */
    access_flags= O_WRONLY | O_BINARY | O_APPEND;
    /* small cache size as frequent flushes */
    cache_size= IO_SIZE;
  }
  else
  {
    DBUG_ASSERT(type == MI_LOG_PHYSICAL);
    log= &myisam_physical_log;
    /* We want to fail if file exists */
    access_flags= O_WRONLY | O_BINARY | O_TRUNC | O_EXCL;
    /*
      We want a large IO_CACHE to have large contiguous disk writes.
      In many systems this size is affordable. In small embedded ones it is
      not, but would they use this log?
    */
    cache_size= IO_SIZE*256;
  }
  if (!myisam_pid)
    myisam_pid=(ulong) getpid();
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
  pthread_mutex_unlock(&THR_LOCK_myisam_log);
  DBUG_RETURN(error);
}


/**
  Destroy's a log's IO_CACHE (for logical or physical log).

  @param  type             physical or logical

  @return Operation status
    @retval 0      ok
    @retval !=0    error
*/

static int mi_log_close_cache(enum enum_mi_log_type type)
{
  int error= 0;
  IO_CACHE *log;
  DBUG_ENTER("mi_log_close_cache");
  pthread_mutex_lock(&THR_LOCK_myisam_log);
  if (type == MI_LOG_LOGICAL)
    log         = &myisam_logical_log;
  else
  {
    DBUG_ASSERT(type == MI_LOG_PHYSICAL);
    log         = &myisam_physical_log;
  }
  if (my_b_inited(log))
  {
    if (end_io_cache(log) ||
        my_close(log->file,MYF(MY_WME)))
      error= my_errno;
    log->file= -1;
  }
  pthread_mutex_unlock(&THR_LOCK_myisam_log);
  DBUG_RETURN(error);
}


/**
  Logs a MyISAM command and its return code to log.

  If this is a physical log and MI_LOG_OPEN has not already been stored for
  this MYISAM_SHARE in this log, also writes a MI_LOG_OPEN.

  @param  log              pointer to the log's IO_CACHE
  @param  command          MyISAM command (see code for allowed commands)
  @param  info_or_share    MI_INFO if logical log, MYISAM_SHARE if physical
  @param  buffert          usually argument to the command (e.g. name of file
                           to open for MI_LOG_OPEN), may be NULL
  @param  length           length of buffert (0 if NULL)
  @param  result           return code of the command
*/

void _myisam_log_command(IO_CACHE *log, enum myisam_log_commands command,
                         void *info_or_share,
                         const uchar *buffert, uint length, int result)
{
  uchar header[14];
  int error, old_errno, headerlen;
  ulong pid=(ulong) GETPID();
  my_bool logical= (log == &myisam_logical_log);
  MYISAM_SHARE *share;
  File file;

  /*
    Speed in online backup (physical log) matters more than in debugging
    (logical log) so we use unlikely().
  */
  if (unlikely(logical))
  {
    file= ((MI_INFO *)info_or_share)->dfile;
    LINT_INIT(share);
  }
  else
  {
    file= (share= (MYISAM_SHARE *)info_or_share)->kfile;
    LINT_INIT(error);
  }

  DBUG_ASSERT(command == MI_LOG_OPEN  || command == MI_LOG_DELETE ||
              command == MI_LOG_CLOSE || command == MI_LOG_EXTRA ||
              command == MI_LOG_LOCK  || command == MI_LOG_DELETE_ALL);
  old_errno=my_errno;
  DBUG_ASSERT(((uint)result) <= UINT_MAX16);
  if (file >= UINT_MAX16 || length >= UINT_MAX16)
  {
    header[0]= ((uchar) command) | MI_LOG_BIG_NUMBERS;
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
    Reasons to not use THR_LOCK_myisam to serialize log writes:
    - better concurrency (not stealing THR_LOCK_myisam which is used for opens
    and closes including long table flushes)
    - mi_close() flushes indexes while holding THR_LOCK_myisam, and that flush
    can cause log writes, so we would lock the mutex twice.
  */
  pthread_mutex_lock(&THR_LOCK_myisam_log);
  /*
    We need to check that 'log' is not closed, this can happen for a physical
    log. Indeed we do not have full control on the table from the thread doing
    mi_log_stop_physical(); it could be an inconsistent logging stop (in
    the middle of writes) or even a consistent one (table can be in
    mi_lock_database(F_UNLCK) and thus want to flush its header)). Log might
    just have been closed while the table still has physical_logging true.
  */
  if (likely(my_b_inited(log) != NULL))
  {
    if (!logical)
    {
      if (command == MI_LOG_OPEN)
      {
        /*
          If there could be two concurrent writers on a MyISAM table, it could
          be that they both do a myisam_log_command(c) where c!=MI_LOG_OPEN,
          which both see MI_LOG_OPEN_stored_in_physical_log false, and both
          call myisam_log_command(MI_LOG_OPEN); we would then have to make one
          single winner: one will run before the other, the other should
          notice MI_LOG_OPEN_stored_in_physical_log became true and back off.
          But there is always at most one writer to a MyISAM table, so the
          assertion below should always be ok
        */
        DBUG_ASSERT(!share->MI_LOG_OPEN_stored_in_physical_log);
        share->MI_LOG_OPEN_stored_in_physical_log= TRUE;
        /*
          We must keep the mutex between setting the boolean above and writing
          to the log ; one instant after unlocking the mutex, the log may be
          closed and so it would be wrong to say that the MI_LOG_OPEN is in
          the log (it would possibly influence a next physical log).
        */
      }
      else if (unlikely(!share->MI_LOG_OPEN_stored_in_physical_log))
      {
        DBUG_ASSERT(command != MI_LOG_CLOSE);
        pthread_mutex_unlock(&THR_LOCK_myisam_log);
        _myisam_log_command(&myisam_physical_log, MI_LOG_OPEN, share,
                            (uchar *)share->unresolv_file_name,
                            strlen(share->unresolv_file_name), 0);
        goto retry;
      }
    }
    else
      error=my_lock(log->file,F_WRLCK,0L,F_TO_EOF,MYF(MY_SEEK_NOT_DONE));
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
    /*
      Another process (if external locking) may want to append to the logical
      log as soon as we unlock its file, we must flush it. Physical log is not
      share-able, does not need to flush.
    */
    if (logical)
    {
      flush_io_cache(log);
      if (!error)
        error=my_lock(log->file,F_UNLCK,0L,F_TO_EOF,MYF(MY_SEEK_NOT_DONE));
    }
  }
  pthread_mutex_unlock(&THR_LOCK_myisam_log);
  my_errno=old_errno;
}


/**
  Logs a MyISAM command (involving a record: MI_LOG_WRITE etc) and its
  return code to the logical log.

  @param  command          MyISAM command (MI_LOG_UPDATE|WRITE)
  @param  info             MI_INFO
  @param  record           record to write/update/etc, not NULL
  @param  filepos          offset in data file where record starts
  @param  result           return code of the command
*/

void _myisam_log_record_logical(enum myisam_log_commands command,
                                MI_INFO *info, const uchar *record,
                                my_off_t filepos, int result)
{
  uchar header[22],*pos;
  int error,old_errno;
  uint length, headerlen;
  ulong pid=(ulong) GETPID();

  DBUG_ASSERT(command == MI_LOG_UPDATE || command == MI_LOG_WRITE);
  old_errno=my_errno;
  if (!info->s->base.blobs)
    length=info->s->base.reclength;
  else
    length=info->s->base.reclength+ _mi_calc_total_blob_length(info,record);
  DBUG_ASSERT(((uint)result) <= UINT_MAX16);
  if (info->dfile >= UINT_MAX16 || filepos >= UINT_MAX32 ||
      length >= UINT_MAX16)
  {
    header[0]= ((uchar) command) | MI_LOG_BIG_NUMBERS;
    DBUG_ASSERT(info->dfile < (2<<24));
    mi_int3store(header + 1, info->dfile);
    mi_int4store(header + 4, pid);
    mi_int2store(header + 8, result);
    mi_sizestore(header + 10, filepos);
    mi_int4store(header + 18, length);
    headerlen= 22;
  }
  else
  {
    header[0]= (uchar) command;
    mi_int2store(header + 1, info->dfile);
    mi_int4store(header + 3, pid);
    mi_int2store(header + 7, result);
    mi_int4store(header + 9, filepos);
    mi_int2store(header + 13, length);
    headerlen= 15;
  }

  pthread_mutex_lock(&THR_LOCK_myisam_log);
  error= my_lock(myisam_logical_log.file, F_WRLCK, 0L, F_TO_EOF,
                 MYF(MY_SEEK_NOT_DONE));
  (void) my_b_write(&myisam_logical_log, header, headerlen);
  (void) my_b_write(&myisam_logical_log, record, info->s->base.reclength);
  if (info->s->base.blobs)
  {
    MI_BLOB *blob,*end;

    for (end=info->blobs+info->s->base.blobs, blob= info->blobs;
	 blob != end ;
	 blob++)
    {
      memcpy_fixed(&pos, record+blob->offset+blob->pack_length,
                   sizeof(char*));
      (void) my_b_write(&myisam_logical_log, pos, blob->length);
    }
  }
  flush_io_cache(&myisam_logical_log);
  if (!error)
    error= my_lock(myisam_logical_log.file, F_UNLCK, 0L, F_TO_EOF,
                   MYF(MY_SEEK_NOT_DONE));
  pthread_mutex_unlock(&THR_LOCK_myisam_log);
  my_errno=old_errno;
}


/* THE FOLLOWING FUNCTIONS SERVE ONLY FOR PHYSICAL LOGGING */

/**
  Logs a my_pwrite() (done to data or index file) to the physical log.

  Also logs MI_LOG_OPEN if first time. Thus, a MI_INFO will write MI_LOG_OPEN
  to the log only if it is doing a write to the table: a table which does
  only reads logs nothing to the physical log.

  @param  command          MyISAM command (MI_LOG_WRITE_BYTES_TO_MYD|MYI)
  @param  share            table's share
  @param  buffert          argument to the pwrite
  @param  length           length of buffer
  @param  filepos          offset in file where buffer was written

  @note length may be small (for example, if updating only a numeric field of
  a record, it could be only a few bytes), so we try to minimize the header's
  size of the log entry (no 'pid', no 'result').
*/

void myisam_log_pwrite_physical(enum myisam_log_commands command,
                                MYISAM_SHARE *share, const uchar *buffert,
                                uint length, my_off_t filepos)
{
  uchar header[21];
  int old_errno, headerlen;
  DBUG_ENTER("myisam_log_pwrite_physical");
  DBUG_ASSERT(command == MI_LOG_WRITE_BYTES_MYD ||
              command == MI_LOG_WRITE_BYTES_MYI);
  DBUG_ASSERT(buffert != NULL && length > 0);
  old_errno= my_errno;
  if (share->kfile >= UINT_MAX16 || filepos >= UINT_MAX32 ||
      length >= UINT_MAX16)
  {
    header[0]= ((uchar) command) | MI_LOG_BIG_NUMBERS;
    DBUG_ASSERT(share->kfile < (2<<24));
    mi_int3store(header + 1, share->kfile);
    mi_sizestore(header + 4, filepos);
    mi_int4store(header + 12, length);
    headerlen= 16;
  }
  else
  {
    header[0]= (uchar) command;
    mi_int2store(header + 1, share->kfile);
    mi_int4store(header + 3, filepos);
    mi_int2store(header + 7, length);
    headerlen= 9;
  }
  /* pid and result are not needed */
retry:
  pthread_mutex_lock(&THR_LOCK_myisam_log);
  if (likely(my_b_inited(&myisam_physical_log) != NULL))
  {
    if (unlikely(!share->MI_LOG_OPEN_stored_in_physical_log))
    {
      pthread_mutex_unlock(&THR_LOCK_myisam_log);
      _myisam_log_command(&myisam_physical_log, MI_LOG_OPEN, share,
                          (uchar *)share->unresolv_file_name,
                          strlen(share->unresolv_file_name), 0);
      goto retry;
    }
    (void) my_b_write(&myisam_physical_log, header, headerlen);
    (void) my_b_write(&myisam_physical_log, buffert, length);
  }
  pthread_mutex_unlock(&THR_LOCK_myisam_log);
  my_errno= old_errno;
  DBUG_VOID_RETURN;
}


/**
  Logs a my_chsize() done to the index file to the physical log.

  Also logs MI_LOG_OPEN if first time.

  @param  share            table's share
  @param  new_length       new length of the table's index file
*/

void myisam_log_chsize_kfile_physical(MYISAM_SHARE *share,
                                      my_off_t new_length)
{
  uchar header[12];
  int old_errno, headerlen;
  DBUG_ENTER("myisam_log_chsize_kfile_physical");
  old_errno= my_errno;
  if (share->kfile >= UINT_MAX16 || new_length >= UINT_MAX32)
  {
    header[0]= MI_LOG_CHSIZE_MYI | MI_LOG_BIG_NUMBERS;
    DBUG_ASSERT(share->kfile < (2<<24));
    mi_int3store(header + 1, share->kfile);
    mi_sizestore(header + 4, new_length);
    headerlen= 12;
  }
  else
  {
    header[0]= MI_LOG_CHSIZE_MYI;
    mi_int2store(header + 1, share->kfile);
    mi_int4store(header + 3, new_length);
    headerlen= 7;
  }
  /* pid and result are not needed */
retry:
  pthread_mutex_lock(&THR_LOCK_myisam_log);
  if (likely(my_b_inited(&myisam_physical_log) != NULL))
  {
    if (unlikely(!share->MI_LOG_OPEN_stored_in_physical_log))
    {
      pthread_mutex_unlock(&THR_LOCK_myisam_log);
      _myisam_log_command(&myisam_physical_log, MI_LOG_OPEN, share,
                          (uchar *)share->unresolv_file_name,
                          strlen(share->unresolv_file_name), 0);
      goto retry;
    }
    (void) my_b_write(&myisam_physical_log, header, headerlen);
  }
  pthread_mutex_unlock(&THR_LOCK_myisam_log);
  my_errno= old_errno;
  DBUG_VOID_RETURN;
}


/**
  Starts MyISAM physical logging for a set of tables.

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
    if ((atomic read of MYISAM_SHARE::physical_logging) != 0)
      write log record to physical log;
  }
  @endcode

  The present function sets MYISAM_SHARE::physical_logging to 1 using an
  atomic write. Atomic write happens before or after atomic read above, and
  atomic read sees the latest value. If before, change will be in the log. If
  after, it is also after the write_to_data_or_index_file and thus change
  will be in the copy. So correctness is always guaranteed. Note the
  importance of checking MYISAM_SHARE::logging always _after_
  write_to_data_or_index_file, with an _atomic_read_ for the reasoning to
  hold.

  @param  log_filename     Name of the physical log file to create
  @param  tables           Hash of names of tables for which we want logging

  @return Operation status
    @retval 0      ok
    @retval !=0    error
*/

static int mi_log_start_physical(const char *log_filename, const HASH *tables)
{
  LIST *list_item;
  int error;
  DBUG_ENTER("mi_log_start_physical");
  DBUG_ASSERT(log_filename != NULL);
  DBUG_ASSERT(hash_inited(tables));

  pthread_mutex_lock(&THR_LOCK_myisam);
  if (mi_log_tables_physical) /* physical logging already running */
  {
    pthread_mutex_unlock(&THR_LOCK_myisam);
    DBUG_ASSERT(0); /* because it should not happen */
    DBUG_RETURN(1);
  }
  mi_log_tables_physical= tables;

  if (unlikely(mi_log_open_cache(MI_LOG_PHYSICAL, log_filename)))
  {
    error= 1;
    goto end;
  }
  /* Go through all open MyISAM tables */
  for (list_item= myisam_open_list; list_item; list_item= list_item->next)
  {
    MI_INFO *info= (MI_INFO*)list_item->data;
    MYISAM_SHARE *share= info->s;
    DBUG_PRINT("info",("table '%s' 0x%lx tested against hash",
                       share->unique_file_name, (ulong)info));
    if (!hash_search(mi_log_tables_physical, (uchar *)share->unique_file_name,
                     share->unique_name_length))
      continue;
    /* Backup kernel shouldn't ask for temporary table's backup */
    DBUG_ASSERT(!share->temporary);
    /*
      We don't need to flush key blocks, WRITE_CACHE or the state
      because every time they are written to disk (at the latest in
      mi_log_stop_physical()) they check for physical logging
      (key cache always has log_key_page_flush_physical() as
      post_write, WRITE_CACHE always has log_flushed_write_cache_physical()
      has post_write, even when _not_ in backup), so any now cached info will
      finally reach the log.
      Conversely, if we wanted to register no callback in key cache and
      WRITE_CACHE when no backup is running (to save function calls
      and atomic reads when no backup is running), we would have to
      flush key cache and WRITE_CACHE here.
    */
    mi_set_physical_logging_state(info->s, 1);
  }
  error= 0;
end:
  pthread_mutex_unlock(&THR_LOCK_myisam);
  if (unlikely(error))
    mi_log_stop_physical(MI_LOG_ACTION_CLOSE_INCONSISTENT, MI_LOG_PHYSICAL,
                         NULL, NULL);
  DBUG_RETURN(error);
}


/**
  Stops MyISAM physical logging.

  As part of this stop operation, user can request that the physical log ends
  in a consistent state, i.e. that it contains copies of the currently cached
  key pages etc. To be consistent assumes that the caller has relevant tables
  write-locked, indeed otherwise the log could end in the middle of a
  statement, and applying it would produce a likely corrupted table.Online
  backup needs such a consistent log to be able to create consistent table
  copies from the log. If online backup is being cancelled, then there is no
  need that the physical log be consistent.

  @param  action           MI_LOG_ACTION_CLOSE_CONSISTENT or
                           MI_LOG_ACTION_CLOSE_INCONSISTENT.

  @return Operation status
    @retval 0      ok
    @retval !=0    error

  @note Even if MI_LOG_ACTION_CLOSE_CONSISTENT, tables may be being written
  now (in practice caller has read-locked tables, but those tables may be
  just going out of a write (after thr_unlock(), before or inside
  mi_lock_database(F_UNLCK) which may be flushing the index header or index
  pages).
*/

static int mi_log_stop_physical(enum enum_mi_log_action action)
{
  int error= 0;
  LIST *list_item;
  DBUG_ENTER("mi_log_stop_physical");
  DBUG_ASSERT(action == MI_LOG_ACTION_CLOSE_CONSISTENT ||
              action == MI_LOG_ACTION_CLOSE_INCONSISTENT);

  pthread_mutex_lock(&THR_LOCK_myisam);
  if (mi_log_tables_physical == NULL) /* no physical logging running */
  {
    pthread_mutex_unlock(&THR_LOCK_myisam);
    DBUG_RETURN(0); /* it's ok if it happens */
  }
  /*
    This is a pointer to a object provided by the caller through
    mi_log_start_physical(); such object is to be freed by the caller.
  */
  mi_log_tables_physical= NULL;

  if (action == MI_LOG_ACTION_CLOSE_CONSISTENT)
  {
    /**
      @todo consider an algorithm which would not keep THR_LOCK_myisam for the
      time of flushing all these tables' indices; we could do a first loop
      with THR_LOCK_myisam to collect shares and "pin" them; then a second
      loop without THR_LOCK_myisam, flushing and unpinning them.
    */
    for (list_item= myisam_open_list; list_item; list_item= list_item->next)
    {
      MI_INFO *info= (MI_INFO*)list_item->data;
      MYISAM_SHARE *share= info->s;
      /*
        Setting of the variable below always happens under THR_LOCK_myisam,
        which we have here, so we don't need atomic operations to read here.
      */
      if (!share->physical_logging)
        continue;
      /*
        Must take intern_lock, at least because key cache isn't safe if two
        calls to flush_key_blocks_int() run concurrently on the same file.
      */
      pthread_mutex_lock(&share->intern_lock);
      /*
        It is possible that some statement just finished, has not called
        mi_lock_database(F_UNLCK) yet, and so some key blocks would still be
        in memory even if !delay_key_write. So we have to flush below even
        in this case, to put them into the log.

        It is also possible (same scenario) that some WRITE_CACHE is not
        flushed yet. This should not happen but it does (can just be a
        forgotten mi_extra(HA_EXTRA_NO_CACHE)); so mi_close() and
        mi_lock_database(F_UNLCK) flush the cache; so we have to do it here
        too, to put the data into the log. Mutices in mi_close() and
        mi_lock_database() ensure that they don't flush at the same time as us
        (which could corrupt the cache). Nobody should flush the WRITE_CACHE
        without a write-lock or intern_lock (see assertion in mi_reset()).

        It is also possible (same scenario) that the index's header has not
        been written yet and nobody is going to do it for us; indeed this can
        happen (two concurrent threads): thread1 has just done
        mi_lock_database(F_WRLCK), is blocked by the thr_lock of our caller,
        thread2 has finished its write statement and is going to execute
        mi_lock_database(F_UNLCK); no index header flush will be done by the
        mi_lock_database(F_UNLCK) of thread2 as w_locks is >0 (due to
        thread1). And no index header flush will be done by thread1 as it is
        blocked. So, we need to flush the index header here, to put it into
        the log.

        Of course, for the flushing above to reach the log, it has to be done
        before setting share->physical_logging to false and before closing the
        log.
      */
      if ((mi_log_index_pages_physical &&
           (share->kfile >= 0) &&
           flush_key_blocks(share->key_cache, share->kfile, FLUSH_KEEP)) ||
          ((info->opt_flag & WRITE_CACHE_USED) &&
           flush_io_cache(&info->rec_cache)) ||
          (share->changed && mi_remap_file_and_write_state_for_unlock(info)))
      {
        error= 1; /* we continue, because log has to be closed anyway */
        mi_print_error(share, HA_ERR_CRASHED);
        mi_mark_crashed(info);		/* Mark that table must be checked */
      }
      pthread_mutex_unlock(&share->intern_lock);
    } /* ... for (list_item=...) */
  } /* ... if (action == MI_LOG_ACTION_CLOSE_CONSISTENT) */

  /*
    Online backup wants to pick this log with my_read() calls, to send it to
    the backup stream. So we don't delete log but close it now, so that its
    IO_CACHE goes to disk (so that all log is visible to the my_read()
    calls). Another reason related to concurrency is mentioned below.
  */
  if (mi_log_close_cache(MI_LOG_PHYSICAL))
    error= 1;

  for (list_item= myisam_open_list; list_item; list_item= list_item->next)
  {
    MYISAM_SHARE *share= ((MI_INFO*)list_item->data)->s;
    /*
      Setting of the variable below always happens under THR_LOCK_myisam,
      which we have here, so we don't need atomic operations to read here.
    */
    if (!share->physical_logging)
      continue;
    mi_set_physical_logging_state(share, 0);
    /*
      We reset MI_LOG_OPEN_stored_in_physical_log. How is this safe with a
      concurrent logging operation (like myisam_log_pwrite_physical()) which
      may want to set it to TRUE at the same time?
      The concurrent logging operation runs either before or after log closing
      (serialization ensured by THR_LOCK_myisam_log). If before, it is before
      us (us==resetter), because log closing is before us, so we win. If
      after, the concurrent logging operation finds the log closed and so
      will not change MI_LOG_OPEN_stored_in_physical_log (so we win again).
      Note the importance of closing the log before, for the reasoning to
      hold.
    */
    share->MI_LOG_OPEN_stored_in_physical_log= FALSE;
  }

  pthread_mutex_unlock(&THR_LOCK_myisam);
  /*
    From this moment on, from the point of view of MyISAM, a new physical log
    (a new backup) can start (new log will use a different tmp name).
  */
  DBUG_RETURN(error);
}
