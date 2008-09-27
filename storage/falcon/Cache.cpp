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

// Cache.cpp: implementation of the Cache class.
//
//////////////////////////////////////////////////////////////////////

#include <memory.h>
#include <stdio.h>
#include "Engine.h"
#include "Cache.h"
#include "BDB.h"
#include "Dbb.h"
#include "Page.h"
#include "IndexPage.h"
#include "PageInventoryPage.h"
#include "Sync.h"
#include "Log.h"
#include "LogLock.h"
#include "Stream.h"
#include "PageWriter.h"
#include "SQLError.h"
#include "Thread.h"
#include "Threads.h"
#include "DatabaseCopy.h"
#include "Database.h"
#include "Bitmap.h"
#include "Priority.h"
#include "SectorCache.h"

#define PARAMETER_UINT(_name, _text, _min, _default, _max, _flags, _update_function) \
	extern uint falcon_##_name;
#define PARAMETER_BOOL(_name, _text, _default, _flags, _update_function) \
	extern char falcon_##_name;
#include "StorageParameters.h"
#undef PARAMETER_UINT
#undef PARAMETER_BOOL
extern uint falcon_io_threads;

//#define STOP_PAGE		55
//#define CACHE_TRACE_FILE	"cache.trace"

#ifdef CACHE_TRACE_FILE
static FILE			*traceFile;
#endif // CACHE_TRACE_FILE

static const uint64 cacheHunkSize		= 1024 * 1024 * 128;
static const int	ASYNC_BUFFER_SIZE	= 1024000;
static const int	sectorCacheSize		= 20000000;
#ifdef _DEBUG
#undef THIS_FILE
static const char THIS_FILE[]=__FILE__;
#endif

//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////

Cache::Cache(Database *db, int pageSz, int hashSz, int numBuffers)
{
	openTraceFile();
	database = db;
	panicShutdown = false;
	pageSize = pageSz;

	unsigned int highBit;
	for (highBit=0x01; highBit < (unsigned int)hashSz; highBit= highBit << 1) { }

	// if there are more than 4096 buckets then lets round down
	// else lets round up
	if (highBit >= 0x00001000) {
		// use power of two rounded down
		hashSize = highBit << 1;
	} else {
		// use power of two rounded up
		hashSize = highBit;
	}

	hashMask = hashSize - 1;
	numberBuffers = numBuffers;
	upperFraction = numberBuffers / 4;
	bufferAge = 0;
	firstDirty = NULL;
	lastDirty = NULL;
	pageWriter = NULL;
	hashTable = new Bdb* [hashSize];
	memset (hashTable, 0, sizeof (Bdb*) * hashSize);
#ifdef DEBUG_SYNC_HASH_TABLE_SIZE
    syncHashTable = new SyncObject [DEBUG_SYNC_HASH_TABLE_SIZE];
	for (int loop = 0; loop < DEBUG_SYNC_HASH_TABLE_SIZE; loop ++)
		syncHashTable[loop].setName("Cache::syncHashTable");
#else /* DEBUG_SYNC_HASH_TABLE_SIZE */
    syncHashTable = new SyncObject [hashSize];
	for (int loop = 0; loop < hashSize; loop ++)
		{
		char tmpName[128];
		snprintf(tmpName,120,"Cache::syncHashTable[%d]",loop);
		syncHashTable[loop].setName(tmpName);
		}
#endif /* DEBUG_SYNC_HASH_TABLE_SIZE */
	if (falcon_use_sectorcache)
		sectorCache = new SectorCache(sectorCacheSize / SECTOR_BUFFER_SIZE, pageSize);

	uint64 n = ((uint64) pageSize * numberBuffers + cacheHunkSize - 1) / cacheHunkSize;
	numberHunks = (int) n;
	bufferHunks = new char* [numberHunks];
	memset(bufferHunks, 0, numberHunks * sizeof(char*));
	syncObject.setName("Cache::syncObject");
	syncFlush.setName("Cache::syncFlush");
	syncDirty.setName("Cache::syncDirty");
	syncThreads.setName("Cache::syncThreads");
	syncWait.setName("Cache::syncWait");
	bufferQueue.syncObject.setName("Cache::bufferQueue.syncObject");

	flushBitmap = new Bitmap;
	numberIoThreads = falcon_io_threads;
	ioThreads = new Thread*[numberIoThreads];
	memset(ioThreads, 0, numberIoThreads * sizeof(ioThreads[0]));
	flushing = false;
	
	try
		{
		// non-protected access to bdbs,endBdbs is OK during initialization
		bdbs = new Bdb [numberBuffers];
		endBdbs = bdbs + numberBuffers;
		int remaining = 0;
		int hunk = 0;
		int allocated = 0;
		char *stuff = NULL;

		for (Bdb *bdb = bdbs; bdb < endBdbs; ++bdb, --remaining)
			{
			if (remaining == 0)
				{
				remaining = MIN(numberBuffers - allocated, (int) (cacheHunkSize / pageSize));
				stuff = bufferHunks[hunk++] = new char [pageSize * (remaining + 1)];
				stuff = (char*) (((UIPTR) stuff + pageSize - 1) / pageSize * pageSize);
				allocated += remaining;
				}

			bdb->cache = this;
			// non-protected access to bufferQueue is OK during initialization
			bufferQueue.append(bdb);
			bdb->buffer = (Page*) stuff;
			stuff += pageSize;
			}
		}
	catch(...)
		{
		delete [] bdbs;

		for (int n = 0; n < numberHunks; ++n)
			delete [] bufferHunks[n];

		delete [] bufferHunks;

		throw;
		}

	validateCache();

	for (int n = 0; n < numberIoThreads; ++n)
		ioThreads[n] = database->threads->start("Cache::Cache", &Cache::ioThread, this);
}

Cache::~Cache()
{

	closeTraceFile();

	delete [] hashTable;
	delete [] syncHashTable;
	delete [] bdbs;
	delete [] ioThreads;
	delete flushBitmap;
	if (falcon_use_sectorcache)
		delete sectorCache;
	
	if (bufferHunks)
		{
		for (int n = 0; n < numberHunks; ++n)
			delete [] bufferHunks[n];

		delete[] bufferHunks;
		}
}

Bdb* Cache::probePage(Dbb *dbb, int32 pageNumber)
{
	ASSERT (pageNumber >= 0);
	Bdb *bdb;
	
	/* If we already have a buffer for this, we're done */
	bdb = lockFindBdbIncrementUseCount(dbb, pageNumber);
	if (bdb)
		{
		if (bdb->buffer->pageType == PAGE_free)
			{
			bdb->decrementUseCount(REL_HISTORY);
			
			return NULL;
			}

		bdb->addRef(Shared  COMMA_ADD_HISTORY);
		bdb->decrementUseCount(REL_HISTORY);

		return bdb;
		}

	return NULL;
}

Bdb* Cache::findBdb(Dbb* dbb, int32 pageNumber, int slot)
{
	for (Bdb *bdb = hashTable [slot]; bdb; bdb = bdb->hash)
{
		if (bdb->pageNumber == pageNumber && bdb->dbb == dbb)
			{
			return bdb;
			}
		}

	return NULL;
}

Bdb* Cache::findBdb(Dbb* dbb, int32 pageNumber)
{
	return (findBdb(dbb, pageNumber, PAGENUM_2_SLOT(pageNumber)));
}

Bdb* Cache::lockFindBdbIncrementUseCount(Dbb* dbb, int32 pageNumber)
{
	int slot = PAGENUM_2_SLOT(pageNumber);
	Sync lockHash (&syncHashTable[PAGENUM_2_LOCK_INDEX(pageNumber, slot)], "Cache::lockFindBdbIncrementUseCount");
	lockHash.lock (Shared);
	Bdb *bdb;

	bdb = findBdb(dbb, pageNumber, slot);
	if (bdb != NULL)
		bdb->incrementUseCount(ADD_HISTORY);

	return bdb;
}

Bdb* Cache::lockFindBdbIncrementUseCount(int32 pageNumber, int slot)
{
	Sync lockHash (&syncHashTable[PAGENUM_2_LOCK_INDEX(pageNumber, slot)], "Cache::lockFindBdbIncrementUseCount");
	lockHash.lock (Shared);

	for (Bdb *bdb = hashTable [slot]; bdb; bdb = bdb->hash)
		if (bdb->pageNumber == pageNumber)
		{
			bdb->incrementUseCount(ADD_HISTORY);
			return bdb;
		}

	return NULL;
}

Bdb* Cache::fetchPage(Dbb *dbb, int32 pageNumber, PageType pageType, LockType lockType)
{
	if (panicShutdown)
		{
		Thread *thread = Thread::getThread("Cache::fetchPage");
		
		if (thread->pageMarks == 0)
			throw SQLError(RUNTIME_ERROR, "Emergency shut is underway");
		}

#ifdef STOP_PAGE			
		if (pageNumber == STOP_PAGE)
 			Log::debug("fetching page %d/%d\n", pageNumber, dbb->tableSpaceId);
#endif

	ASSERT (pageNumber >= 0);
	Bdb *bdb;

	/* If we already have a buffer for this, we're done */
	bdb = lockFindBdbIncrementUseCount(dbb, pageNumber);
	if (!bdb)
		{
		// getFreeBuffer() locks a hash bucket to remove the candidate bdb
		// if we locked our hash bucket before the call then we could have
		// a deadlock
		// thus we get the free buffer before we lock the hash bucket we will
		// be inserting into.  This avoids a dead lock but generates a race
		// we take care of the race by reversing the getFreeBuffer() work
		// when we lose the race
		Bdb *bdbAvailable;
		int slot = PAGENUM_2_SLOT(pageNumber);
		Sync lockHash (&syncHashTable[PAGENUM_2_LOCK_INDEX(pageNumber, slot)], "Cache::fetchPage");

		bdbAvailable = getFreeBuffer();
		/* assume we'll be inserting this new BDB.  Set new page number. */
		bdbAvailable->pageNumber = pageNumber;
		bdbAvailable->dbb = dbb;

		lockHash.lock(Exclusive);
		bdb = findBdb(dbb, pageNumber, slot);
		if (!bdb)
			{
			// we won the race so lets use the free bdb
			// relink into hash table
			bdbAvailable->hash = hashTable [slot];
			hashTable [slot] = bdbAvailable;
			lockHash.unlock();

			bdb = bdbAvailable;
#ifdef STOP_PAGE			
			if (bdb->pageNumber == STOP_PAGE)
				Log::debug("reading page %d/%d\n", bdb->pageNumber, dbb->tableSpaceId);
#endif
			
			Priority priority(database->ioScheduler);
			priority.schedule(PRIORITY_MEDIUM);	
			if (falcon_use_sectorcache)
				sectorCache->readPage(bdb);
			else
				dbb->readPage(bdb);
			priority.finished();
#ifdef HAVE_PAGE_NUMBER
			ASSERT(bdb->buffer->pageNumber == pageNumber);
#endif			
			if (Exclusive != lockType)
				bdb->downGrade(lockType);
			}
			else
			{
			//syncObject.validateExclusive("Cache::fetchPage (retry)");
			bdb->incrementUseCount(ADD_HISTORY);
			lockHash.unlock();
			bdb->addRef(lockType  COMMA_ADD_HISTORY);
			bdb->decrementUseCount(REL_HISTORY);
			moveToHead(bdb);

			// lost a race.  put our available back to useable
			// side effect, bdbAvailable will have to age again before we re-use it.
			bdbAvailable->hash = NULL;
			bdbAvailable->pageNumber = -1;
			bdbAvailable->dbb = NULL;
			bdbAvailable->release(REL_HISTORY);
			}
		}
		else
		{
		bdb->addRef(lockType  COMMA_ADD_HISTORY);
		bdb->decrementUseCount(REL_HISTORY);
		moveToHead(bdb);
		}

	Page *page = bdb->buffer;
	
	/***
	if (page->checksum != (short) pageNumber)
		FATAL ("page %d wrong page number, got %d\n",
				 bdb->pageNumber, page->checksum);
	***/
	
	if (pageType && page->pageType != pageType)
		{
		/*** future code
		bdb->release(REL_HISTORY);
		throw SQLError (DATABASE_CORRUPTION, "page %d wrong page type, expected %d got %d\n",
						pageNumber, pageType, page->pageType);
		***/
		FATAL ("page %d/%d wrong page type, expected %d got %d\n",
				 bdb->pageNumber, dbb->tableSpaceId, pageType, page->pageType);
		}

	ASSERT (bdb->pageNumber == pageNumber);
	ASSERT (bdb->dbb == dbb);
	ASSERT (bdb->useCount > 0);

	return bdb;
}

Bdb* Cache::fakePage(Dbb *dbb, int32 pageNumber, PageType type, TransId transId)
{
	Bdb *bdb;

#ifdef STOP_PAGE			
	if (pageNumber == STOP_PAGE)
		Log::debug("faking page %d/%d\n",pageNumber, dbb->tableSpaceId);
#endif

	/* If we already have a buffer for this, we're done */
	bdb = lockFindBdbIncrementUseCount(dbb, pageNumber);
	if (!bdb)
		{
		// getFreeBuffer() locks a hash bucket to remove the candidate bdb
		// if we locked our hash bucket before the call then we could have
		// a deadlock
		// thus we get the free buffer before we lock the hash bucket we will
		// be inserting into.  This avoids a dead lock but generates a race
		// we take care of the race by reversing the getFreeBuffer() work
		// when we lose the race
		Bdb *bdbAvailable;
		int slot = PAGENUM_2_SLOT(pageNumber);
		Sync lockHash (&syncHashTable[PAGENUM_2_LOCK_INDEX(pageNumber, slot)], "Cache::fetchPage");

		bdbAvailable = getFreeBuffer();
		/* assume we'll be inserting this new BDB.  Set new page number. */
		bdbAvailable->pageNumber = pageNumber;
		bdbAvailable->dbb = dbb;

		lockHash.lock(Exclusive);
		bdb = findBdb(dbb, pageNumber, slot);
		if (!bdb)
			{
			// we won the race so lets use the free bdb
			// relink into hash table
			bdbAvailable->hash = hashTable [slot];
			hashTable [slot] = bdbAvailable;
			lockHash.unlock();

			bdb = bdbAvailable;
			}
			else
			{
			//syncObject.validateExclusive("Cache::fetchPage (retry)");
			bdb->incrementUseCount(ADD_HISTORY);
			lockHash.unlock();
			bdb->addRef(Exclusive  COMMA_ADD_HISTORY);
			bdb->decrementUseCount(REL_HISTORY);
			moveToHead(bdb);

			// lost a race.  put our available back to useable
			// side effect, bdbAvailable will have to age again before we re-use it.
			bdbAvailable->hash = NULL;
			bdbAvailable->pageNumber = -1;
			bdbAvailable->dbb = NULL;
			bdbAvailable->release(REL_HISTORY);
			}
		}
		else
		{
		bdb->addRef(Exclusive  COMMA_ADD_HISTORY);
		bdb->decrementUseCount(REL_HISTORY);
		moveToHead(bdb);
		}

	if (!dbb->isReadOnly)
		bdb->mark(transId);
		
	memset(bdb->buffer, 0, pageSize);
	bdb->setPageHeader(type);

	return bdb;
}

void Cache::flush(int64 arg)
{
	Sync flushLock(&syncFlush, "Cache::flush(1)");
	Sync dirtyLock(&syncDirty, "Cache::flush(2)");
	flushLock.lock(Exclusive);
	
	if (flushing)
		return;

	syncWait.lock(NULL, Exclusive);
	//Log::debug(%d: "Initiating flush\n", dbb->deltaTime);
	flushArg = arg;
	flushPages = 0;
	physicalWrites = 0;
	
	dirtyLock.lock(Shared);
	for (Bdb *bdb = firstDirty; bdb; bdb = bdb->nextDirty)
		{
		bdb->flushIt = true;
		flushBitmap->set(bdb->pageNumber);
		++flushPages;
		}
	dirtyLock.unlock();

	analyzeFlush();

	flushStart = database->timestamp;
	flushing = true;
	flushLock.unlock();
	
	for (int n = 0; n < numberIoThreads; ++n)
		if (ioThreads[n])
			ioThreads[n]->wake();
}

void Cache::moveToHead(Bdb * bdb)
{
	// If buffer has moved out of the upper "fraction" of the LRU queue, move it back up
	// non-protected access to age is harmless since it is fuzzy anyway
	if (bdb->age < bufferAge - (uint64) upperFraction)
		{
		Sync bufferQueueLock (&bufferQueue.syncObject, "Cache::moveToHead");

		bufferQueueLock.lock (Exclusive);
		bdb->age = bufferAge++;
		bufferQueue.remove(bdb);
		bufferQueue.prepend(bdb);
		//validateUnique (bdb);
		}
}

void Cache::moveToHeadAlreadyLocked(Bdb * bdb)
{
	bdb->age = bufferAge++;
	bufferQueue.remove(bdb);
	bufferQueue.prepend(bdb);
	//validateUnique (bdb);
}

Bdb* Cache::getFreeBuffer(void)
{
	Sync bufferQueueLock (&bufferQueue.syncObject, "Cache::getFreeBuffer");
	unsigned int count;
	Bdb *bdb;

	// Find a candidate BDB.
	for (;;)
		{
		bufferQueueLock.lock (Exclusive);
		// find a candidate that is NOT in use and NOT dirty and in the tail fraction of the LRU
		for (count = 0, bdb = bufferQueue.last; bdb ; bdb = bdb->prior, count++)
			{
			if (count >= upperFraction)
				{
				bdb = NULL;
				break;
				}
			if (bdb->useCount == 0)
				{
				if (!bdb->isDirty)
					{
					bdb->incrementUseCount(REL_HISTORY);
					moveToHeadAlreadyLocked(bdb);
					break;
					}
				}
				else
				{
					// get this one out of the way so we don't search it every time
					moveToHeadAlreadyLocked(bdb);
#ifdef CHECK_STALLED_BDB
					bdb->stallCount++;
					if ((bdb->stallCount & 0x03) == 0x03) {
						Log::debug("Page %d is in use and aged %d times\n",
								bdb->pageNumber, bdb->stallCount);
					}
#endif // CHECK_STALLED_BDB
				}
			}
		if (!bdb)
			// find a candidate that is NOT in use, could be dirty
			for (bdb = bufferQueue.last; bdb; bdb = bdb->prior)
				if (bdb->useCount == 0)
					{
					bdb->incrementUseCount(REL_HISTORY);
					moveToHeadAlreadyLocked(bdb);
					break;
					}
		bufferQueueLock.unlock();

		if (!bdb)
			throw SQLError(RUNTIME_ERROR, "buffer pool is exhausted\n");
			
		if (bdb->pageNumber >= 0)
		{
			int	slotRemove = PAGENUM_2_SLOT(bdb->pageNumber);
			Sync lockHashRemove (&syncHashTable[PAGENUM_2_LOCK_INDEX(bdb->pageNumber, slotRemove)], "Cache::getFreeBuffer");
			lockHashRemove.lock(Exclusive);

			if (bdb->useCount != 1)
				{
				// we lost a race try again
				bdb->decrementUseCount(REL_HISTORY);
				lockHashRemove.unlock();
				continue;
				}

			if (bdb->isDirty)
				writePage (bdb, WRITE_TYPE_REUSE);

			/* Unlink its old incarnation from the page/hash table */
			for (Bdb **ptr = hashTable + PAGENUM_2_SLOT(bdb->pageNumber) ;; ptr = &(*ptr)->hash)
				if (*ptr == bdb)
					{
					*ptr = bdb->hash;
					break;
					}
				else
					ASSERT (*ptr);
		}

		break;
		}
#ifdef CHECK_STALLED_BDB
	bdb->stallCount = 0;
#endif // CHECK_STALLED_BDB

#ifdef COLLECT_BDB_HISTORY
	bdb->initHistory();
#endif
	bdb->addRef (Exclusive  COMMA_ADD_HISTORY);
	bdb->decrementUseCount(REL_HISTORY);

	return bdb;
}

void Cache::validate()
{
	//Sync bufferQueueLock (&bufferQueue.syncObject, "Cache::validate");

	//bufferQueueLock.lock (Shared);
	// non-protected access to bufferQueue is DANGEROUS...
	for (Bdb *bdb = bufferQueue.last; bdb; bdb = bdb->prior)
		{
		//IndexPage *page = (IndexPage*) bdb->buffer;
		ASSERT (bdb->useCount == 0);
		}
}

void Cache::markDirty(Bdb *bdb)
{
	Sync dirtyLock (&syncDirty, "Cache::markDirty");
	dirtyLock.lock (Exclusive);
	bdb->nextDirty = NULL;
	bdb->priorDirty = lastDirty;

	if (lastDirty)
		lastDirty->nextDirty = bdb;
	else
		firstDirty = bdb;

	lastDirty = bdb;
	//validateUnique (bdb);
}

void Cache::markClean(Bdb *bdb)
{
	Sync dirtyLock (&syncDirty, "Cache::markClean");
	dirtyLock.lock (Exclusive);

	/***
	if (bdb->flushIt)
		Log::debug(" Cleaning page %d in %s marked for flush\n", bdb->pageNumber, (const char*) bdb->dbb->fileName);
	***/
	
	bdb->flushIt = false;
	
	if (bdb == lastDirty)
		lastDirty = bdb->priorDirty;

	if (bdb->priorDirty)
		bdb->priorDirty->nextDirty = bdb->nextDirty;

	if (bdb->nextDirty)
		bdb->nextDirty->priorDirty = bdb->priorDirty;

	if (bdb == firstDirty)
		firstDirty = bdb->nextDirty;

	bdb->nextDirty = NULL;
	bdb->priorDirty = NULL;
}

void Cache::writePage(Bdb *bdb, int type)
{
	Sync writer(&bdb->syncWrite, "Cache::writePage(1)");
	writer.lock(Exclusive);

	if (!bdb->isDirty)
		{
		//Log::debug("Cache::writePage: page %d not dirty\n", bdb->pageNumber);
		markClean (bdb);

		return;
		}

	//ASSERT(!(bdb->flags & BDB_write_pending));
	Dbb *dbb = bdb->dbb;
	ASSERT(database);
	markClean (bdb);
	// time_t start = database->timestamp;
	Priority priority(database->ioScheduler);
	priority.schedule(PRIORITY_MEDIUM);

	try
		{
		if (falcon_use_sectorcache)
			sectorCache->writePage(bdb);
		dbb->writePage(bdb, type);
		}
	catch (SQLException& exception)
		{
		priority.finished();

		if (exception.getSqlcode() != DEVICE_FULL)
			throw;

		database->setIOError(&exception);
		Thread *thread = Thread::getThread("Cache::writePage");

		for (bool error = true; error;)
			{
			if (thread->shutdownInProgress)
				return;

			thread->sleep(1000);

			try
				{
				priority.schedule(PRIORITY_MEDIUM);
				dbb->writePage(bdb, type);
				error = false;
				database->clearIOError();
				}
			catch (SQLException& exception2)
				{
				priority.finished();

				if (exception2.getSqlcode() != DEVICE_FULL)
					throw;
				}
			}
		}


	priority.finished();

	/***
	time_t delta = database->timestamp - start;

	if (delta > 1)
		Log::debug("Page %d took %d seconds to write\n", bdb->pageNumber, delta);
	***/

#ifdef STOP_PAGE
	if (bdb->pageNumber == STOP_PAGE)
		Log::debug("writing page %d/%d\n", bdb->pageNumber, dbb->tableSpaceId);
#endif

	bdb->isDirty = false;
	
	if (pageWriter && bdb->isRegistered)
		{
		bdb->isRegistered = false;
		pageWriter->pageWritten(bdb->dbb, bdb->pageNumber);
		}

	if (dbb->shadows)
		{
		Sync cloneLock (&dbb->syncClone, "Cache::writePage(2)");
		cloneLock.lock (Shared);

		for (DatabaseCopy *shadow = dbb->shadows; shadow; shadow = shadow->next)
			shadow->rewritePage(bdb);
		}
}

void Cache::analyze(Stream *stream)
{
	Sync dirtyLock (&syncDirty, "Cache::analyze");
	int inUse = 0;
	int dirty = 0;
	int dirtyList = 0;
	int total = 0;
	Bdb *bdb;

	// non-protected access to bdbs,endBdbs is DANGEROUS...
	for (bdb = bdbs; bdb < endBdbs; ++bdb)
		{
		++total;
		
		if (bdb->isDirty)
			++dirty;
			
		if (bdb->useCount)
			++inUse;
		}

	dirtyLock.lock (Shared);
	for (bdb = firstDirty; bdb; bdb = bdb->nextDirty)
		++dirtyList;
	dirtyLock.unlock();

	stream->format ("Cache: %d pages, %d in use, %d dirty, %d in dirty chain\n",
					total, inUse, dirty, dirtyList);
}

void Cache::validateUnique(Bdb *target)
{
	int	slot = PAGENUM_2_SLOT(target->pageNumber);

	// WARNING: unlocked walk of hash table.... DANGEROUS
	for (Bdb *bdb = hashTable [slot]; bdb; bdb = bdb->hash)
		ASSERT (bdb == target || !(bdb->pageNumber == target->pageNumber && bdb->dbb == target->dbb));
}

void Cache::freePage(Dbb *dbb, int32 pageNumber)
{
	int slot = PAGENUM_2_SLOT(pageNumber);
	Sync lockHash (&syncHashTable[PAGENUM_2_LOCK_INDEX(pageNumber, slot)], "Cache::freePage");
	lockHash.lock(Shared);

	// If page exists in cache (usual case), clean it up

	for (Bdb *bdb = hashTable [slot]; bdb; bdb = bdb->hash)
		if (bdb->pageNumber == pageNumber && bdb->dbb == dbb)
			{
			if (bdb->isDirty)
				{
				markClean (bdb);
				}
				
			bdb->isDirty = false;
			break;
			}
}

void Cache::flush(Dbb *dbb)
{
	//Sync sync (&syncDirty, "Cache::flush(1)");
	//sync.lock (Exclusive);
	Sync sync (&syncObject, "Cache::flush(3)");
	sync.lock (Shared);

	for (Bdb *bdb = bdbs; bdb < endBdbs; ++bdb)
		if (bdb->dbb == dbb)
			{
			if (bdb->isDirty)
				writePage(bdb, WRITE_TYPE_FORCE);

			bdb->dbb = NULL;
			}
}

bool Cache::hasDirtyPages(Dbb *dbb)
{
	Sync dirtyLock (&syncDirty, "Cache::hasDirtyPages");
	dirtyLock.lock (Shared);

	for (Bdb *bdb = firstDirty; bdb; bdb = bdb->nextDirty)
		if (bdb->dbb == dbb)
			{
			return true;
			}

	return false;
}


void Cache::setPageWriter(PageWriter *writer)
{
	pageWriter = writer;
}

void Cache::validateCache(void)
{
	//MemMgrValidate(bufferSpace);
}

Bdb* Cache::trialFetch(Dbb* dbb, int32 pageNumber, LockType lockType)
{
	if (panicShutdown)
		{
		Thread *thread = Thread::getThread("Cache::trialFetch");
		
		if (thread->pageMarks == 0)
			throw SQLError(RUNTIME_ERROR, "Emergency shut is underway");
		}

	ASSERT (pageNumber >= 0);
	Bdb *bdb;

	/* If we already have a buffer for this, we're done */
	bdb = lockFindBdbIncrementUseCount(dbb, pageNumber);
	if (bdb)
		{
		bdb->addRef(lockType  COMMA_ADD_HISTORY);
		bdb->decrementUseCount(REL_HISTORY);
		moveToHead(bdb);
	}

	return bdb;
}

void Cache::syncFile(Dbb *dbb, const char *text)
{
	const char *fileName = dbb->fileName;
	int writes = dbb->writesSinceSync;
	time_t start = database->timestamp;
	dbb->sync();
	
	if (Log::isActive(LogInfo))
		{
		time_t delta = database->timestamp - start;
		
		if (delta > 1)
			Log::log(LogInfo, "%d: %s %s sync: %d pages in %d seconds\n", database->deltaTime, fileName, text, writes, delta);
		}
}

void Cache::ioThread(void* arg)
{
	((Cache*) arg)->ioThread();
}

void Cache::ioThread(void)
{
	Sync syncThread(&syncThreads, "Cache::ioThread");
	syncThread.lock(Shared);
	Sync flushLock(&syncFlush, "Cache::ioThread");
	Priority priority(database->ioScheduler);
	Thread *thread = Thread::getThread("Cache::ioThread");
	UCHAR *rawBuffer = new UCHAR[ASYNC_BUFFER_SIZE];
	UCHAR *buffer = (UCHAR*) (((UIPTR) rawBuffer + pageSize - 1) / pageSize * pageSize);
	UCHAR *end = (UCHAR*) ((UIPTR) (rawBuffer + ASYNC_BUFFER_SIZE) / pageSize * pageSize);
	flushLock.lock(Exclusive);
	
	// This is the main loop.  Write blocks until there's nothing to do, then sleep
	
	for (;;)
		{
		int32 pageNumber = flushBitmap->nextSet(0);
		int count;
		
		if (pageNumber >= 0)
			{
			Bdb *bdb;
			Dbb *dbb;
			int	slot = PAGENUM_2_SLOT(pageNumber);
			bool hit = false;
			Bdb *bdbList = NULL;
			UCHAR *p = buffer;
			
			// Look for the page to flush.
			bdb = lockFindBdbIncrementUseCount(pageNumber, slot);
			if (bdb && bdb->flushIt && bdb->isDirty)
				{
				hit = true;
				count = 0;
				dbb = bdb->dbb;
				
				flushBitmap->clear(pageNumber);
				
				// get all his friends
				while (p < end)
					{
					++count;
					bdb->addRef(Shared  COMMA_ADD_HISTORY);
					
					bdb->syncWrite.lock(NULL, Exclusive);
					bdb->ioThreadNext = bdbList;
					bdbList = bdb;
					
					//ASSERT(!(bdb->flags & BDB_write_pending));
					//bdb->flags |= BDB_write_pending;
					memcpy(p, bdb->buffer, pageSize);
					p += pageSize;
					bdb->flushIt = false;
					markClean(bdb);
					bdb->isDirty = false;
					bdb->release(REL_HISTORY);
					
					bdb = lockFindBdbIncrementUseCount(dbb, bdb->pageNumber + 1);
					if (!bdb)
						break;
					
					if (!bdb->isDirty && !continueWrite(bdb))
						{
						bdb->decrementUseCount(REL_HISTORY);
						break;
						}
					}
				
				flushLock.unlock();
				//Log::debug(" %d Writing %s %d pages: %d - %d\n", thread->threadId, (const char*) dbb->fileName, count, pageNumber, pageNumber + count - 1);
				int length = p - buffer;
				priority.schedule(PRIORITY_LOW);
				
				try
					{
					priority.schedule(PRIORITY_LOW);
					dbb->writePages(pageNumber, length, buffer, WRITE_TYPE_FLUSH);
					}
				catch (SQLException& exception)
					{
					priority.finished();
					
					if (exception.getSqlcode() != DEVICE_FULL)
						throw;
					
					database->setIOError(&exception);
					
					for (bool error = true; error;)
						{
						if (thread->shutdownInProgress)
							{
							Bdb *next;

							for (bdb = bdbList; bdb; bdb = next)
								{
								//bdb->flags &= ~BDB_write_pending;
								next = bdb->ioThreadNext;
								bdb->syncWrite.unlock();
								bdb->decrementUseCount(REL_HISTORY);
								}
								
							return;
							}
						
						thread->sleep(1000);
						
						try
							{
							priority.schedule(PRIORITY_LOW);
							dbb->writePages(pageNumber, length, buffer, WRITE_TYPE_FLUSH);
							error = false;
							database->clearIOError();
							}
						catch (SQLException& exception2)
							{
							priority.finished();
							
							if (exception2.getSqlcode() != DEVICE_FULL)
								throw;
							}
						}
					}

				priority.finished();
				Bdb *next;

				for (bdb = bdbList; bdb; bdb = next)
					{
					//ASSERT(bdb->flags & BDB_write_pending);
					//bdb->flags &= ~BDB_write_pending;
					next = bdb->ioThreadNext;
					bdb->syncWrite.unlock();
					bdb->decrementUseCount(REL_HISTORY);
					}
				
				flushLock.lock(Exclusive);
				++physicalWrites;
				
				}
			else
				{
					if (bdb)
						bdb->decrementUseCount(REL_HISTORY);
				}
			
			if (!hit)
				{
				flushBitmap->clear(pageNumber);
				}
			}
		else 
			{
			if (flushing)
				{
				int writes = physicalWrites;
				int pages = flushPages;
				int delta = (int) (database->timestamp - flushStart);
				int64 callbackArg = flushArg;
				flushing = false;
				flushArg = 0;
				flushLock.unlock();
				syncWait.unlock();
				
				if (writes > 0 && Log::isActive(LogInfo))
					Log::log(LogInfo, "%d: Cache flush: %d pages, %d writes in %d seconds (%d pps)\n",
								database->deltaTime, pages, writes, delta, pages / MAX(delta, 1));

				if (callbackArg != 0)
					database->pageCacheFlushed(callbackArg);
				}
			else
				flushLock.unlock();
			
			if (thread->shutdownInProgress)
				break;

			thread->sleep();
			flushLock.lock(Exclusive);
		}
		} // for ever
	
	delete [] rawBuffer;			
}

bool Cache::continueWrite(Bdb* startingBdb)
{
	Dbb *dbb = startingBdb->dbb;
	int clean = 1;
	int dirty = 0;
	
	for (int32 pageNumber = startingBdb->pageNumber + 1, end = pageNumber+ 5; pageNumber < end; ++pageNumber)
		{
		Bdb *bdb;
		
		if (dirty > clean)
			return true;

		bdb = lockFindBdbIncrementUseCount(dbb, pageNumber);
		if (!bdb)
			return dirty >= clean;
		
		if (bdb->isDirty)
			++dirty;
		else
			++clean;
		bdb->decrementUseCount(REL_HISTORY);
		}
	
	return (dirty >= clean);
}

void Cache::shutdown(void)
{
	shutdownThreads();
	Sync sync (&syncDirty, "Cache::shutdown");
	sync.lock (Exclusive);

	for (Bdb *bdb = firstDirty; bdb; bdb = bdb->nextDirty)
		bdb->dbb->writePage(bdb, WRITE_TYPE_SHUTDOWN);
}

void Cache::shutdownNow(void)
{
	panicShutdown = true;
	shutdown();
}

void Cache::shutdownThreads(void)
{
	for (int n = 0; n < numberIoThreads; ++n)
		{
		ioThreads[n]->shutdown();
		ioThreads[n] = 0;
		}
	
	Sync lockThreads(&syncThreads, "Cache::shutdownThreads");
	lockThreads.lock(Exclusive);
}

#ifdef CACHE_TRACE_FILE
void Cache::analyzeFlush(void)
{
	Dbb *dbb = NULL;
	Bdb *bdb;
	Sync dirtyLock (&syncDirty, "Cache::hasDirtyPages");
	
	dirtyLock.lock (Shared);
	for (bdb = firstDirty; bdb; bdb = bdb->nextDirty)
		if (bdb->dbb->tableSpaceId == 1)
			{
			dbb = bdb->dbb;
			
			break;
			}
	dirtyLock.unlock();
	
	if (!dbb)
		return;
	
	fprintf(traceFile, "-------- time %d -------\n", database->deltaTime);

	for (int pageNumber = 0; (pageNumber = flushBitmap->nextSet(pageNumber)) >= 0;)
		// non-protected access to hash table via findBdb()!
		if ( (bdb = findBdb(dbb, pageNumber)) )
			{
			int start = pageNumber;
			int type = bdb->buffer->pageType;
			
			// non-protected access to hash table via findBdb()!
			for (; (bdb = findBdb(dbb, ++pageNumber)) && bdb->flushIt;)
				;
			
			fprintf(traceFile, " %d flushed: %d to %d, first type %d\n", pageNumber - start, start, pageNumber - 1, type);
			
			// non-protected access to hash table via findBdb()!
			for (int max = pageNumber + 5; pageNumber < max && (bdb = findBdb(dbb, pageNumber)) && !bdb->flushIt; ++pageNumber)
				{
				if (bdb->isDirty)
					fprintf(traceFile, "     %d dirty not flushed, type %d \n", pageNumber, bdb->buffer->pageType);
				else
					fprintf(traceFile,"      %d not dirty, type %d\n", pageNumber, bdb->buffer->pageType);
				}
			}
		else
			++pageNumber;
	
	fflush(traceFile);			
}

void Cache::openTraceFile(void)
{
	if (traceFile)
		closeTraceFile();
		
	traceFile = fopen(TRACE_FILE, "a+");
	fprintf(traceFile, "Starting\n");
//KEL
//	setvbuf(traceFile, (char *) NULL, _IOLBF, 0);

}

void Cache::closeTraceFile(void)
{
	if (traceFile)
		{
		fclose(traceFile);
		traceFile = NULL;
		}
}
#else // CACHE_TRACE_FILE
void Cache::analyzeFlush(void)
{
}
void Cache::openTraceFile(void)
{
}
void Cache::closeTraceFile(void)
{
}
#endif // CACHE_TRACE_FILE

void Cache::flushWait(void)
{
	Sync waitLock(&syncWait, "Cache::flushWait");
	waitLock.lock(Exclusive);
}

