/* Copyright (C) 2006 MySQL AB

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

// Cache.h: interface for the Cache class.
//
//////////////////////////////////////////////////////////////////////

#if !defined(AFX_CACHE_H__6A019C1F_A340_11D2_AB5A_0000C01D2301__INCLUDED_)
#define AFX_CACHE_H__6A019C1F_A340_11D2_AB5A_0000C01D2301__INCLUDED_

#if _MSC_VER >= 1000
#pragma once
#endif // _MSC_VER >= 1000

#include "Page.h"
#include "SyncObject.h"
#include "Queue.h"

// uncomment DEBUG_SYNC_HASH_TABLE_SIZE to cause more contention and test for race conditions
//#define DEBUG_SYNC_HASH_TABLE_SIZE (0x01 << 1)
#ifdef DEBUG_SYNC_HASH_TABLE_SIZE
#  define DEBUG_SYNC_HASH_TABLE_MASK (DEBUG_SYNC_HASH_TABLE_SIZE - 1)
#  define PAGENUM_2_LOCK_INDEX(_pgnum, _slot) ((_pgnum) & DEBUG_SYNC_HASH_TABLE_MASK)
#else /* DEBUG_SYNC_HASH_TABLE_SIZE */
#  define PAGENUM_2_LOCK_INDEX(_pgnum, _slot) ((_slot))
#endif /* DEBUG_SYNC_HASH_TABLE_SIZE */

#define PAGENUM_2_SLOT(_pgnum) ((_pgnum) & hashMask)

class Bdb;
class Dbb;
class PageWriter;
class Stream;
class Sync;
class Thread;
class Database;
class Bitmap;
class SectorCache;

class Cache  
{
public:
	void	shutdownNow(void);
	void	shutdown(void);
	Bdb*	probePage(Dbb *dbb, int32 pageNumber);
	void	setPageWriter (PageWriter *writer);
	bool	hasDirtyPages (Dbb *dbb);
	void	flush (Dbb *dbb);
	void	freePage (Dbb *dbb, int32 pageNumber);
	void	validateUnique (Bdb *bdb);
	void	analyze (Stream *stream);
	void	writePage (Bdb *bdb, int type);
	void	markClean (Bdb *bdb);
	void	markDirty (Bdb *bdb);
	void	validate();
	void	moveToHead (Bdb *bdb);
	void	flush(int64 arg);
	void	validateCache(void);
	void	syncFile(Dbb *dbb, const char *text);
	void	ioThread(void);
	void	shutdownThreads(void);
	bool	continueWrite(Bdb* startingBdb);
	
	static void ioThread(void* arg);
		
	Bdb*		fakePage (Dbb *dbb, int32 pageNumber, PageType type, TransId transId);
	Bdb*		fetchPage (Dbb *dbb, int32 pageNumber, PageType type, LockType lockType);
	Bdb*		trialFetch(Dbb* dbb, int32 pageNumber, LockType lockType);

	void		analyzeFlush(void);
	static void	openTraceFile(void);
	static void	closeTraceFile(void);

	Cache(Database *db, int pageSize, int hashSize, int numberBuffers);
	virtual ~Cache();

	SyncObject	syncObject;
	PageWriter	*pageWriter;
	Database	*database;
	int			numberBuffers;
	bool		panicShutdown;
	bool		flushing;

protected:
	Bdb*		getFreeBuffer(void);
	Bdb*		findBdb(Dbb* dbb, int32 pageNumber, int slot);
	Bdb*		findBdb(Dbb* dbb, int32 pageNumber);
	Bdb*		lockFindBdbIncrementUseCount(Dbb* dbb, int32 pageNumber);
	Bdb*		lockFindBdbIncrementUseCount(int32 pageNumber, int slot);

	int64		flushArg;
	Bdb			*bdbs;
	Bdb			*endBdbs;
	Queue<Bdb>	bufferQueue;
	Bdb			**hashTable;
	SyncObject  *syncHashTable;
	Bdb			*firstDirty;
	Bdb			*lastDirty;
	Bitmap		*flushBitmap;
	char		**bufferHunks;
	Thread		**ioThreads;
	SectorCache	*sectorCache;
	SyncObject	syncFlush;
	SyncObject	syncDirty;
	SyncObject	syncThreads;
	SyncObject	syncWait;
	time_t		flushStart;
	int			flushPages;
	int			physicalWrites;
	int			hashSize;
	unsigned int	hashMask;
	int			pageSize;
	unsigned int upperFraction;
	int			numberHunks;
	int			numberDirtyPages;
	int			numberIoThreads;
	volatile uint64 bufferAge;
public:
	void flushWait(void);
};

#endif // !defined(AFX_CACHE_H__6A019C1F_A340_11D2_AB5A_0000C01D2301__INCLUDED_)
