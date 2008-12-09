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

// SyncObject.cpp: implementation of the SyncObject class.
//
//////////////////////////////////////////////////////////////////////

#include <stdio.h>
#include <string.h>
#include <memory.h>

#ifdef _WIN32
#include <windows.h>
#undef ERROR
#undef ASSERT
#undef TRACE
#endif

#ifdef FALCONDB
#define TRACE
#else
#define ASSERT(b)
#endif

#include "Engine.h"
#include "SyncObject.h"
#include "Thread.h"
#include "Threads.h"
#include "Sync.h"
#include "SyncHandler.h"
#include "Interlock.h"
#include "LinkedList.h"
#include "Log.h"
#include "LogLock.h"
#include "SQLError.h"
#include "Stream.h"
#include "InfoTable.h"

//#define FAST_SHARED
//#define STALL_THRESHOLD	1000

#define BACKOFF	\
		if (thread)\
			backoff(thread);\
		else\
			{ thread = Thread::getThread("SyncObject::lock"); thread->backoff = BACKOFF_INTERVAL; }

#define BACKOFF_INTERVAL	(thread->random % 1000)

#ifdef TRACE_SYNC_OBJECTS

#define BUMP(counter)							++counter
#define BUMP_INTERLOCKED(counter)				INTERLOCKED_INCREMENT(counter)

static const int				MAX_SYNC_OBJECTS = 300000;
static volatile INTERLOCK_TYPE	nextSyncObjectId;
static SyncObject				*syncObjects[MAX_SYNC_OBJECTS];

#else
#define BUMP(counter)
#define BUMP_INTERLOCKED(counter)
#endif

#ifdef _DEBUG
#undef THIS_FILE
static const char THIS_FILE[]=__FILE__;
#endif

#ifdef EMULATE

static int cas_emulation (volatile int *state, int compare, int exchange);

#undef COMPARE_EXCHANGE
#define COMPARE_EXCHANGE(target,compare,exchange) \
	(cas_emulation(target,compare,exchange) == compare)


int cas_emulation (volatile int *state, int compare, int exchange)
{
	int result = *state;

	if (result == compare)
		*state = exchange;
	
	return result;
}
#endif

//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////

SyncObject::SyncObject()
{
	readers = 0;
	waiters = 0;
	lockState = 0;
	queue = NULL;
	monitorCount = 0;
	stalls = 0;
	exclusiveThread = NULL;

#ifdef TRACE_SYNC_OBJECTS
	sharedCount = 0;
	collisionCount = 0;
	exclusiveCount = 0;
	waitCount = 0;
	queueLength = 0;
	location = NULL;
	name = NULL;
	objectId = INTERLOCKED_INCREMENT(nextSyncObjectId);
	
	if (objectId < MAX_SYNC_OBJECTS)
		syncObjects[objectId] = this;
#endif
}

SyncObject::~SyncObject()
{
	ASSERT(lockState == 0);
	
#ifdef TRACE_SYNC_OBJECTS
	if (objectId < MAX_SYNC_OBJECTS)
		syncObjects[objectId] = NULL;
#endif
}

#ifdef FAST_SHARED
void SyncObject::lock(Sync *sync, LockType type, int timeout)
{
	Thread *thread;

#ifdef TRACE_SYNC_OBJECTS
	location = (sync ? sync->location : NULL);

#ifdef USE_FALCON_SYNC_HANDLER
	SyncHandler *syncHandler = getFalconSyncHandler();
	if (sync && syncHandler)
		syncHandler->addLock(this, location, type);
#endif
#endif

	// Shared case
	
	if (type == Shared)
		{
		thread = NULL;
		BUMP_INTERLOCKED(sharedCount);
		INTERLOCKED_INCREMENT(readers);
		
		// If there aren't any writers, we've got the lock.  Ducky.
		
		if (lockState == 0)
			{
			DEBUG_FREEZE;
			
			return;
			}

		// See if we have already have the lock, in which case bump the monitor and get going
		
		if (!thread)
			thread = Thread::getThread("SyncObject::lock");

		if (thread == exclusiveThread)
			{
			INTERLOCKED_DECREMENT(readers);
			++monitorCount;
			DEBUG_FREEZE;
			
			return;
			}

		// We have contention.  Get the mutex and prepare the wait, but
		// maybe we'll get lucky
					
		mutex.lock();

		if (lockState == 0)
			{
			DEBUG_FREEZE;
			
			return;
			}

		// If there isn't an exclusive thread or the exclusive thread is stalled, we've got the lock
		
		if (!exclusiveThread || exclusiveThread == queue)
			{
			DEBUG_FREEZE;
			
			return;
			}
		
		// There is an outstanding exclusive lock; wait
		
		INTERLOCKED_DECREMENT(readers);
		bumpWaiters(1);
		wait(type, thread, sync, timeout);
		DEBUG_FREEZE;
		
		return;
		}
	
	// Exclusive case
	
	thread = Thread::getThread("SyncObject::lock");
	thread->backoff = BACKOFF_INTERVAL;
	ASSERT(thread);

	// If we're already the exclusive thread, just bump the monitor count and we're done
	
	if (thread == exclusiveThread)
		{
		++monitorCount;
		BUMP(exclusiveCount);
		DEBUG_FREEZE;
		
		return;
		}

	// If nothing is pending, go for the lock.
	
	while (readers == 0 && waiters == 0)
		{
		INTERLOCK_TYPE oldState = lockState;
		
		if (oldState != 0)
			break;
			
		if (COMPARE_EXCHANGE(&lockState, oldState, -1))
			{
			exclusiveThread = thread;

			if (readers)
				{
				mutex.lock();
				
				if (readers)
					{
					if (queue && queue->lockType == Shared)
						BUMP(exclusiveCount);
					bumpWaiters(1);
					wait(type, thread, sync, timeout);
					}
				
				}
				
			BUMP(exclusiveCount);
			DEBUG_FREEZE;
			
			return; 
			}
		
		BACKOFF;
		}
		
	mutex.lock();
	bumpWaiters(1);
	BUMP(exclusiveCount);
	
	while (readers == 0 && queue == NULL)
		{
		INTERLOCK_TYPE oldState = lockState;
		
		if (oldState != 0)
			break;
			
		if (COMPARE_EXCHANGE(&lockState, oldState, -1))
			{
			exclusiveThread = thread;

			if (readers)
				{
				wait(type, thread, sync, timeout);
				DEBUG_FREEZE;
				
				return;
				}

			bumpWaiters(-1);
			mutex.release();
			DEBUG_FREEZE;
			
			return;
			}
		
		BACKOFF;
		}

	// mutex is held going into wait() It is released before coming out.

	wait(type, thread, sync, timeout);
	DEBUG_FREEZE;
}

#else	// FAST_SHARED

// Old (aka working) version

void SyncObject::lock(Sync *sync, LockType type, int timeout)
{
	Thread *thread;

#ifdef TRACE_SYNC_OBJECTS
	location = (sync ? sync->location : NULL);

#ifdef USE_FALCON_SYNC_HANDLER
	SyncHandler *syncHandler = getFalconSyncHandler();
	if (sync && syncHandler)
		syncHandler->addLock(this, location, type);
#endif
#endif

	if (type == Shared)
		{
		thread = NULL;
		//BUMP_INTERLOCKED(sharedCount);

		for(;;)
			{
			INTERLOCK_TYPE oldState = lockState;

			if (oldState < 0)
				break;

			INTERLOCK_TYPE newState = oldState + 1;

			if (COMPARE_EXCHANGE(&lockState, oldState, newState))
				{
				DEBUG_FREEZE;
				return;
				}
				
			BUMP_INTERLOCKED(collisionCount);
			BACKOFF;
			}

		mutex.lock();
		bumpWaiters(1);

		for(;;)
			{
			INTERLOCK_TYPE oldState = lockState;

			if (oldState < 0)
				break;
				
			INTERLOCK_TYPE newState = oldState + 1;

			if (COMPARE_EXCHANGE(&lockState, oldState, newState))
				{
				bumpWaiters(-1);
				mutex.release();
				DEBUG_FREEZE;
				
				return;
				}

			BACKOFF;
			}

		if (!thread)
			thread = Thread::getThread("SyncObject::lock");

		if (thread == exclusiveThread)
			{
			++monitorCount;
			bumpWaiters(-1);
			mutex.release();
			DEBUG_FREEZE;
			
			return;
			}
		}
	else
		{
		thread = Thread::getThread("SyncObject::lock");
		thread->backoff = BACKOFF_INTERVAL;
		ASSERT(thread);

		if (thread == exclusiveThread)
			{
			++monitorCount;
			BUMP(exclusiveCount);
			DEBUG_FREEZE;
			
			return;
			}

		while (waiters == 0)
			{
			INTERLOCK_TYPE oldState = lockState;
			
			if (oldState != 0)
				break;
				
			if (COMPARE_EXCHANGE(&lockState, oldState, -1))
				{
				exclusiveThread = thread;
				BUMP(exclusiveCount);
				DEBUG_FREEZE;
				
				return; 
				}
			
			BACKOFF;
			}
			
		mutex.lock();
		bumpWaiters(1);
		BUMP(exclusiveCount);
		
		while (queue == NULL)
			{
			INTERLOCK_TYPE oldState = lockState;
			
			if (oldState != 0)
				break;
				
			if (COMPARE_EXCHANGE(&lockState, oldState, -1))
				{
				exclusiveThread = thread;
				bumpWaiters(-1);
				mutex.release();
				DEBUG_FREEZE;
				
				return;
				}
			
			BACKOFF;
			}
		}

	// mutex is held going into wait() It is released before coming out.

	wait(type, thread, sync, timeout);
	DEBUG_FREEZE;
}
#endif // FAST_SHARED

#ifdef FAST_SHARED
void SyncObject::unlock(Sync *sync, LockType type)
{
	if (monitorCount)
		{
		//ASSERT (monitorCount > 0);
		--monitorCount;
		DEBUG_FREEZE;

		return;
		}
	
	if (type == Shared)
		{
		ASSERT(readers > 0);
		
		if (INTERLOCKED_DECREMENT(readers) == 0 && waiters)
			grantLocks();
			
		return;
		}
	
	ASSERT(lockState == -1 && exclusiveThread != queue);
	Thread *thread = NULL;
	
	for (;;)
		{
		//ASSERT (type == Exclusive && lockState == -1);
		long oldState = lockState;
		long newState = (type == Shared) ? oldState - 1 : 0;
		exclusiveThread = NULL;
		
		if (COMPARE_EXCHANGE(&lockState, oldState, newState))
			{
			DEBUG_FREEZE;

			if (waiters)
				grantLocks();
				
			return;
			}
			
		BACKOFF;
		}

	DEBUG_FREEZE;
}

#else // FAST_SHARED
void SyncObject::unlock(Sync *sync, LockType type)
{
#if defined TRACE_SYNC_OBJECTS && defined USE_FALCON_SYNC_HANDLER
	SyncHandler *syncHandler = findFalconSyncHandler();
	if (sync && syncHandler)
		syncHandler->delLock(this);
#endif

	//ASSERT(lockState != 0);
	
	if (monitorCount)
		{
		//ASSERT (monitorCount > 0);
		--monitorCount;
		DEBUG_FREEZE;

		return;
		}
	
	Thread *thread = NULL;
	
	for (;;)
		{
		//ASSERT ((type == Shared && lockState > 0) || (type == Exclusive && lockState == -1));
		long oldState = lockState;
		long newState = (type == Shared) ? oldState - 1 : 0;
		exclusiveThread = NULL;
		
		if (COMPARE_EXCHANGE(&lockState, oldState, newState))
			{
			DEBUG_FREEZE;

			if (waiters)
				grantLocks();
				
			return;
			}
			
		BACKOFF;
		}

	DEBUG_FREEZE;
}
#endif

#ifdef FAST_SHARED

void SyncObject::downGrade(LockType type)
{
	ASSERT (monitorCount == 0);
	ASSERT (type == Shared);
	ASSERT (lockState == -1);
	INTERLOCKED_INCREMENT(readers);
	
	for (;;)
		if (COMPARE_EXCHANGE(&lockState, -1, 0))
			{
			exclusiveThread = NULL;
			DEBUG_FREEZE;
			
			if (waiters)
				grantLocks();

			return;
			}
}

#else // FAST_SHARED

void SyncObject::downGrade(LockType type)
{
	ASSERT (monitorCount == 0);
	ASSERT (type == Shared);
	ASSERT (lockState == -1);
	
	for (;;)
		if (COMPARE_EXCHANGE(&lockState, -1, 1))
			{
			exclusiveThread = NULL;
			DEBUG_FREEZE;
			
			if (waiters)
				grantLocks();

			return;
			}
}
#endif //FAST_SHARED

void SyncObject::wait(LockType type, Thread *thread, Sync *sync, int timeout)
{
	// mutex is currently held

	++stalls;
	BUMP(waitCount);

#ifdef STALL_THRESHOLD
	if ((stalls % STALL_THRESHOLD) == STALL_THRESHOLD - 1)
		frequentStaller(thread, sync);
#endif

	Thread *volatile *ptr;
	
	for (ptr = &queue; *ptr; ptr = &(*ptr)->queue)
		{
		BUMP(queueLength);
		
		if (*ptr == thread)
			{
			LOG_DEBUG ("Apparent single thread deadlock for thread %d (%x)\n", thread->threadId, thread);
			
			for (Thread *thread = queue; thread; thread = thread->queue)
				thread->print();
				
			mutex.release();
			throw SQLEXCEPTION (BUG_CHECK, "Single thread deadlock");
			}
		}

	thread->queue = NULL;
	thread->lockType = type;
	*ptr = thread;
	thread->lockGranted = false;
	thread->lockPending = sync;
	++thread->activeLocks;
	bool wokeup = 0;

	if (timeout)
		while (!thread->lockGranted)
			{
			wokeup = thread->sleep (timeout, &mutex);
			
			if (thread->lockGranted)
				{
				mutex.release();
				return;
				}
			
			for (ptr = &queue; *ptr; ptr = &(*ptr)->queue)
				if (*ptr == thread)
					{
					*ptr = thread->queue;
					--waiters;
					break;
					}
			
			if (!wokeup)
				{
				mutex.release();
				timedout(timeout);
				}
			}
		
	while (!thread->lockGranted)
		{
		wokeup = thread->sleep (10000, &mutex);
		
		if (thread->lockGranted)
			{
			if (!wokeup)
				Log::debug("Apparent lost thread wakeup\n");

			break;
			}
			
		if (!wokeup)
			{
			stalled (thread);
			break;
			}
		}

	mutex.release();

	while (!thread->lockGranted)
		thread->sleep();
}

bool SyncObject::isLocked()
{
	return lockState != 0;
}


void SyncObject::stalled(Thread *thread)
{
#ifdef TRACE
	// assume mutex is already locked.
	LogLock logLock;
	LinkedList threads;
	LinkedList syncObjects;
	thread->findLocks (threads, syncObjects);

	LOG_DEBUG ("Stalled threads\n");

	FOR_OBJECTS (Thread*, thrd, &threads)
		thrd->print();
	END_FOR;

	LOG_DEBUG ("Stalled synchronization objects:\n");

	FOR_OBJECTS (SyncObject*, syncObject, &syncObjects)
		syncObject->print();
	END_FOR;

	LOG_DEBUG ("------------------------------------\n");
#endif
}

void SyncObject::findLocks(LinkedList &threads, LinkedList &syncObjects)
{
#ifdef TRACE
	if (syncObjects.appendUnique(this))
		{
		if (exclusiveThread && exclusiveThread != queue)
			exclusiveThread->findLocks(threads, syncObjects);

		for (Thread *thread = queue; thread; thread = thread->queue)
			thread->findLocks(threads, syncObjects);
		}
#endif
}

void SyncObject::print()
{
#ifdef TRACE
	LOG_DEBUG ("  SyncObject %lx: state %d, readers %d, monitor %d, waiters %d\n", 
				this, lockState, readers, monitorCount, waiters);

	if (exclusiveThread)
		exclusiveThread->print ("    Exclusive thread");

	for (Thread *volatile thread = queue; thread; thread = thread->queue)
		thread->print ("    Waiting thread");
#endif
}


void SyncObject::sysServiceFailed(const char* service, int code)
{
	throw SQLEXCEPTION (BUG_CHECK, "Single thread deadlock");
}

void SyncObject::bumpWaiters(int delta)
{
	if (delta == 1)
		INTERLOCKED_INCREMENT(waiters);
	else if (delta == -1)
		INTERLOCKED_DECREMENT(waiters);
	else
		for (;;)
			{
			INTERLOCK_TYPE oldValue = waiters;
			INTERLOCK_TYPE newValue = waiters + delta;
			
			if (COMPARE_EXCHANGE(&waiters, oldValue, newValue))
				return;
			}
}

#ifdef FAST_SHARED
void SyncObject::grantLocks(void)
{
	mutex.lock();
	ASSERT ((waiters && queue) || (!waiters && !queue));
	const char *description = NULL;
	Thread *thread = NULL;
	
	for (Thread *waiter = queue, *prior = NULL, *next; waiter; waiter = next)
		{
		description = waiter->description;
		bool granted = false;
		next = waiter->queue;
				
		if (waiter->lockType == Shared)
			{
			INTERLOCKED_INCREMENT(readers);
			granted = true;
			}
		else
			{
			ASSERT(waiter->lockType == Exclusive);
			
			if (exclusiveThread == waiter)
				{
				ASSERT(lockState == -1);
				granted = true;
				}
			else
				while (lockState == 0)
					{
					if (COMPARE_EXCHANGE(&lockState, 0, -1))
						{
						granted = true;
						exclusiveThread = waiter;
						break;
						}
					
					BACKOFF;
					}
			}
		
		if (granted)
			{
			if (prior)
				prior->queue = next;
			else
				queue = next;
			
			bool shutdownInProgress = waiter->shutdownInProgress;
			
			if (shutdownInProgress)
				Thread::lockExitMutex();
					
			bumpWaiters(-1);
			--waiter->activeLocks;
			waiter->grantLock (this);
			
			if (shutdownInProgress)
				Thread::unlockExitMutex();
			}
		else
			prior = waiter;
		}
			
	mutex.release();
}

#else // FAST_SHARED

void SyncObject::grantLocks(void)
{
	mutex.lock();
	ASSERT ((waiters && queue) || (!waiters && !queue));
	const char *description = NULL;
	Thread *thread = NULL;
	
	for (Thread *waiter = queue, *prior = NULL, *next; waiter; waiter = next)
		{
		description = waiter->description;
		bool granted = false;
		next = waiter->queue;
				
		if (waiter->lockType == Shared)
			for (int oldState; (oldState = lockState) >= 0;)
				{
				long newState = oldState + 1;
				
				if (COMPARE_EXCHANGE(&lockState, oldState, newState))
					{
					granted = true;
					exclusiveThread = NULL;
					break;
					}
				
				BACKOFF;
				}
		else
			{
			ASSERT(waiter->lockType == Exclusive);
			
			while (lockState == 0)
				{
				if (COMPARE_EXCHANGE(&lockState, 0, -1))
					{
					granted = true;
					exclusiveThread = waiter;
					break;
					}
				
				BACKOFF;
				}
			}
		
		if (granted)
			{
			if (prior)
				prior->queue = next;
			else
				queue = next;
			
			bool shutdownInProgress = waiter->shutdownInProgress;
			
			if (shutdownInProgress)
				Thread::lockExitMutex();
					
			bumpWaiters(-1);
			--waiter->activeLocks;
			waiter->grantLock (this);
			
			if (shutdownInProgress)
				Thread::unlockExitMutex();
			}
		else
			prior = waiter;
		}
			
	mutex.release();
}
#endif // FAST_SHARED

int SyncObject::getState(void)
{
	return lockState;
}

#ifdef FAST_SHARED

void SyncObject::validate(LockType lockType)
{
	switch (lockType)
		{
		case None:
			ASSERT (lockState == 0 && readers == 0);
			break;
		
		case Shared:
			ASSERT (readers > 0 && !(exclusiveThread && exclusiveThread != queue));
			break;
		
		case Exclusive:
			ASSERT (lockState == -1 && (readers == 0 || queue != NULL));
			break;
		
		case Invalid:
			break;
		}
}

#else

void SyncObject::validate(LockType lockType)
{
	switch (lockType)
		{
		case None:
			ASSERT (lockState == 0);
			break;
		
		case Shared:
			ASSERT (lockState > 0);
			break;
		
		case Exclusive:
			ASSERT (lockState == -1);
			break;
		
		case Invalid:
			break;
		}
}
#endif // FAST_SHARED

#ifdef FAST_SHARED
void SyncObject::unlock(void)
{
	if (exclusiveThread && exclusiveThread != queue)
		unlock(NULL, Exclusive);
	else if (readers > 0)
		unlock (NULL, Shared);
	else
		ASSERT(false);
}

#else //FAST_SHARED

void SyncObject::unlock(void)
{
	if (lockState > 0)
		unlock (NULL, Shared);
	else if (lockState == -1)
		unlock (NULL, Exclusive);
	else
		ASSERT(false);
}
#endif //FAST_SHARED

bool SyncObject::ourExclusiveLock(void)
{
	if (lockState != -1)
		return false;

	return exclusiveThread == Thread::getThread("SyncObject::ourExclusiveLock");
}

void SyncObject::frequentStaller(Thread *thread, Sync *sync)
{
	Thread *threadQue = thread->queue;
	LockType lockType = thread->lockType;
	bool lockGranted = thread->lockGranted;
	Sync *lockPending = thread->lockPending;

	if (sync)
		LOG_DEBUG("Frequent stall from %s\n", sync->location);
	else
		LOG_DEBUG("Frequent stall from unknown\n");
		
	thread->queue = threadQue;
	thread->lockType = lockType;
	thread->lockGranted = lockGranted;
	thread->lockPending = lockPending;
}
void SyncObject::analyze(Stream* stream)
{
#ifdef TRACE_SYNC_OBJECTS
	SyncObject *syncObject;
	stream->format("Where\tShares\tExclusives\tWaits\tAverage Queue\n");
	
	for (int n = 1; n < MAX_SYNC_OBJECTS; ++n)
		if ( (syncObject = syncObjects[n]) && syncObject->location)
			stream->format("%s\t%d\t%d\t%d\t%d\t\n",
					syncObject->location,
					syncObject->sharedCount,
					syncObject->exclusiveCount,
					syncObject->waitCount,
					(syncObject->waitCount) ? syncObject->queueLength / syncObject->waitCount : 0);
#endif
}

void SyncObject::dump(void)
{
#ifdef TRACE_SYNC_OBJECTS
	FILE *out = fopen("SyncObject.dat", "w");
	
	if (!out)
		return;
		
	fprintf(out, "Where\tShares\tExclusives\tWaits\tAverage Queue\n");
	SyncObject *syncObject;
	
	for (int n = 1; n < MAX_SYNC_OBJECTS; ++n)
		if ( (syncObject = syncObjects[n]) )
			{
			const char *name = (syncObject->name) ? syncObject->name : syncObject->location;
			
			if (name)
				fprintf(out, "%s\t%d\t%d\t%d\t%d\t\n",
						name,
						syncObject->sharedCount,
						syncObject->exclusiveCount,
						syncObject->waitCount,
						(syncObject->waitCount) ? syncObject->queueLength / syncObject->waitCount : 0);

			syncObject->sharedCount = 0;
			syncObject->exclusiveCount = 0;
			syncObject->waitCount = 0;
			syncObject->queueLength = 0;
			}
	
	fclose(out);
#endif
}

void SyncObject::getSyncInfo(InfoTable* infoTable)
{
	SyncObject *syncObject;
	
	for (int index = 1; index < MAX_SYNC_OBJECTS; ++index)
		if ( (syncObject = syncObjects[index]) && syncObject->location)
			{
			int n = 0;
			infoTable->putString(n++, syncObject->location);
			infoTable->putInt(n++, syncObject->sharedCount);
			infoTable->putInt(n++, syncObject->exclusiveCount);
			infoTable->putInt(n++, syncObject->waitCount);
			int queueLength = (syncObject->waitCount) ? syncObject->queueLength / syncObject->waitCount : 0;
			infoTable->putInt(n++, queueLength);
			infoTable->putRecord();
			}
}

void SyncObject::setName(const char* string)
{
#ifdef TRACE_SYNC_OBJECTS
	name = string;
#endif
}
const char*	SyncObject::getName(void)
{
#ifdef TRACE_SYNC_OBJECTS
	return name;
#else
	return NULL;
#endif
}
const char*	SyncObject::getLocation(void)
{
#ifdef TRACE_SYNC_OBJECTS
	return location;
#else
	return NULL;
#endif
}

void SyncObject::timedout(int timeout)
{
	throw SQLError(LOCK_TIMEOUT, "lock timed out after %d milliseconds\n", timeout);
}

int SyncObject::getCollisionCount(void)
{
	return collisionCount;
}

void SyncObject::backoff(Thread* thread)
{
	//thread->sleep(1);
    int a = 0;

	for (int n = 0; n < thread->backoff; ++n)
		++a;
	
	thread->backoff += a;
}
