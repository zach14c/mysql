/* Copyright (C) 2008 Sun Microsystems, Inc.

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

#ifdef USE_PRAGMA_INTERFACE
#pragma interface                      /* gcc class implementation */
#endif

#ifndef DEBUG_SYNC_H
#define DEBUG_SYNC_H

#include <my_global.h>

class THD;

#ifdef EXTRA_DEBUG
/**
  Sync points allow us to force the server to reach a certain line of code
  and block there until the client tells the server it is ok to go on.
  The client tells the server to block with SELECT GET_LOCK()
  and unblocks it with SELECT RELEASE_LOCK(). Used for debugging difficult
  concurrency problems
*/
#define DBUG_SYNC_POINT(lock_name,lock_timeout) \
 debug_sync_point(lock_name,lock_timeout)
void debug_sync_point(const char* lock_name, uint lock_timeout);
#else
#define DBUG_SYNC_POINT(lock_name,lock_timeout)
#endif /* EXTRA_DEBUG */

/* Debug Sync Facility. */
#if defined(ENABLED_DEBUG_SYNC)
/* Macro to be put in the code at synchronization points. */
#define DEBUG_SYNC(_thd_, _sync_point_name_)                            \
          do { if (unlikely(opt_debug_sync_timeout))                    \
               debug_sync(_thd_, STRING_WITH_LEN(_sync_point_name_));   \
             } while (0)
/* Command line option --debug-sync-timeout. See mysqld.cc. */
extern uint opt_debug_sync_timeout;
/* Default WAIT_FOR timeout if command line option is given without argument. */
#define DEBUG_SYNC_DEFAULT_WAIT_TIMEOUT 300
/* Debug Sync prototypes. See debug_sync.cc. */
extern int  debug_sync_init(void);
extern void debug_sync_end(void);
extern void debug_sync_init_thread(THD *thd);
extern void debug_sync_end_thread(THD *thd);
extern void debug_sync(THD *thd, const char *sync_point_name, size_t name_len);
#else /* defined(ENABLED_DEBUG_SYNC) */
#define DEBUG_SYNC(_thd_, _sync_point_name_)    /* disabled DEBUG_SYNC */
#endif /* defined(ENABLED_DEBUG_SYNC) */

#endif /* DEBUG_SYNC_H */
