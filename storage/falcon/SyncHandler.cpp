/* Copyright (C) 2008 MySQL AB

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

#include <string.h>
#include <stdio.h>
#include <memory.h>
#include "Engine.h"
#include "Log.h"
#include "SyncObject.h"
#include "Sync.h"
#include "SyncHandler.h"
#include "SQLError.h"
#include "Thread.h"
#include "Dbb.h"

extern uint64		falcon_initial_allocation;

#ifdef USE_FALCON_SYNC_HANDLER
static SyncHandler *syncHandler;
static bool initialized = false;
static const char *multipleLockList [] =
	{
	"Bdb::syncWrite",
	NULL
	};
#endif

#ifdef _DEBUG
#undef THIS_FILE
static const char THIS_FILE[]=__FILE__;
#endif

SyncHandler*	getFalconSyncHandler(void)
{
#ifdef USE_FALCON_SYNC_HANDLER
	if (!initialized)
		{
		initialized = true;
		syncHandler = new SyncHandler();
		}
	
	return syncHandler;
#else
	return NULL;
#endif
}

SyncHandler*	findFalconSyncHandler(void)
{
#ifdef USE_FALCON_SYNC_HANDLER
	if (initialized && syncHandler)
		return syncHandler;
#endif
	return NULL;
}


SyncHandler::SyncHandler(void)
{
#ifdef USE_FALCON_SYNC_HANDLER
	syncObjects        = new SyncObjectInfo* [syncObjHashSize];
	locations          = new SyncLocationInfo* [locationHashSize];
	threads            = new SyncThreadInfo* [threadHashSize];
	locationStacks     = new LocationStackInfo* [stackHashSize];
	possibleDeadlocks  = new DeadlockInfo* [deadlockHashSize];


	memset(syncObjects,    0, sizeof(SyncObjectInfo*) * syncObjHashSize);
	memset(locations,      0, sizeof(SyncLocationInfo*) * locationHashSize);
	memset(threads,        0, sizeof(SyncThreadInfo*) * threadHashSize);
	memset(locationStacks, 0, sizeof(LocationStackInfo*) * stackHashSize);
	memset(possibleDeadlocks, 0, sizeof(DeadlockInfo*) * deadlockHashSize);

	syncObjCount = 0;
	locationCount = 0;
	threadCount = 0;
	locationStackCount = 0;
	possibleDeadlockCount = 0;
#endif
}

SyncHandler::~SyncHandler(void)
{
#ifdef USE_FALCON_SYNC_HANDLER
	int n;

	for (n = 0; n < syncObjHashSize; ++n)
		for (SyncObjectInfo *soi; (soi = syncObjects[n]);)
			{
			syncObjects[n] = soi->collision;
			delete soi;
			}

	for (n = 0; n < locationHashSize; ++n)
		for (SyncLocationInfo *loc; (loc = locations[n]);)
			{
			locations[n] = loc->collision;
			delete loc;
			}

	for (n = 0; n < threadHashSize; ++n)
		for (SyncThreadInfo *thd; (thd = threads[n]);)
			{
			threads[n] = thd->collision;
			delete thd;
			}

	for (n = 0; n < stackHashSize; ++n)
		for (LocationStackInfo *stk; (stk = locationStacks[n]);)
			{
			locationStacks[n] = stk->collision;
			delete stk;
			}

	for (n = 0; n < deadlockHashSize; ++n)
		for (DeadlockInfo *dli; (dli = possibleDeadlocks[n]);)
			{
			possibleDeadlocks[n] = dli->collision;
			delete dli;
			}

	delete[] syncObjects;
	delete[] locations;
	delete[] threads;
	delete[] locationStacks;
	delete[] possibleDeadlocks;

	initialized = false;
	syncHandler = NULL;

#endif
}

void SyncHandler::addLock(SyncObject *syncObj, const char* locationName)
{
#ifdef USE_FALCON_SYNC_HANDLER
	if (syncObj == &syncObject)
		return;			// prevent recursion

	Thread *thread = thread = Thread::getThread("SyncHandler::addLock");
	if (locationName == NULL)
		locationName = syncObj->getName();

	Sync sync(&syncObject, "SyncHandler::addLock");
	sync.lock(Exclusive);

	SyncThreadInfo *thd = addThread(thread->threadId);
	SyncObjectInfo *soi = addSyncObject(syncObj->getName());
	SyncLocationInfo *loc = addLocation(locationName, soi);

	addToThread(thd, loc);
#endif
}

void SyncHandler::delLock(SyncObject *syncObj)
{
#ifdef USE_FALCON_SYNC_HANDLER
	if (syncObj == &syncObject)
		return;

	Sync sync(&syncObject, "SyncHandler::delLock");
	sync.lock(Exclusive);

	SyncThreadInfo *thd = findThread();
	SyncObjectInfo * soi = findSyncObject(syncObj->getName());
	if (soi == NULL)
		Log::debug("* Error; %s (%s) could not be deleted from the thread. Not found.\n", syncObj->getLocation(), syncObj->getName());
	else
		delFromThread(thd, soi);
#endif
}

void SyncHandler::dump(void)
{
#ifdef USE_FALCON_SYNC_HANDLER
	Sync sync(&syncObject, "SyncHandler::addLock");
	sync.lock(Exclusive);

	time_t now;
	time(&now);
	Log::debug("== Falcon Deadlock Detector - %24s ==\n", ctime(&now));
	countLocationStacks();
	validate();
	showSyncObjects();
	showLocations();
	showLocationStacks();
	showPossibleDeadlockStacks();
#endif
}

#ifdef USE_FALCON_SYNC_HANDLER

SyncThreadInfo* SyncHandler::findThread()
{
	Thread *thread = thread = Thread::getThread("SyncHandler::delLock");
	int slot = thread->threadId % threadHashSize;
	return findThread(thread->threadId, slot);
}

SyncThreadInfo* SyncHandler::findThread(int thdId, int slot)
{
	for (SyncThreadInfo *thd = threads[slot]; thd; thd = thd->collision)
		if (thd->Id == thdId)
			return thd;

	return NULL;
}
SyncThreadInfo * SyncHandler::addThread(int thdId)
{
	int slot = thdId % threadHashSize;
	SyncThreadInfo *thd = findThread(thdId, slot);

	if (!thd)
		{
		thd = new SyncThreadInfo;
		thd->Id = thdId;
		thd->height = 0;
		thd->isStackRecorded = true;  // No need to record it until it is higher than 2.
		thd->collision = threads[slot];
		memset(thd->stack, 0, sizeof(SyncLocationInfo*) * syncStackSize);
		threads[slot] = thd;
		if ((++threadCount % 100) == 0)
			Log::debug("* SyncHandler has found %d unique threads IDs\n", threadCount);
		}

	ASSERT(thd->collision != thd);
	return thd;
}

SyncObjectInfo *SyncHandler::findSyncObject(const char* syncObjName)
{
	int slot = JString::hash(syncObjName, syncObjHashSize);
	return findSyncObject(syncObjName, slot);
}

SyncObjectInfo *SyncHandler::findSyncObject(const char* syncObjName, int slot)
{
	for (SyncObjectInfo *soi = syncObjects[slot]; soi; soi = soi->collision)
		{
		if (soi->name == syncObjName)
			return soi;
		}

	return NULL;
}

SyncObjectInfo *SyncHandler::addSyncObject(const char* syncObjName)
{
	ASSERT(syncObjName != NULL);
	ASSERT(strlen(syncObjName) != 0);

	int slot = JString::hash(syncObjName, syncObjHashSize);
	SyncObjectInfo *soi = findSyncObject(syncObjName, slot);

	if (!soi)
		{
		soi = new SyncObjectInfo;
		soi->prev = NULL;
		soi->next = NULL;
		soi->name = syncObjName;
		soi->collision = syncObjects[slot];
		syncObjects[slot] = soi;

		soi->multiple = false;
		for (int n = 0; (multipleLockList[n]); n++)
			if (soi->name == multipleLockList[n])
				soi->multiple = true;

		if ((++syncObjCount % 100) == 0)
			Log::debug("* SyncHandler has found %d unique sync objects\n", syncObjCount);
		}

	ASSERT(soi->collision != soi);
	
	return soi;
}

void SyncHandler::showSyncObjects(void)
{
	Log::debug("\n\n== SyncHandler has found %d unique sync objects ==\n", syncObjCount);
	Log::debug("  _____SyncObjectName_____\n", syncObjCount);

	for (int slot = 0; slot < syncObjHashSize; slot++)
		for (SyncObjectInfo *soi = syncObjects[slot]; soi; soi = soi->collision)
			Log::debug("  %s\n", soi->name);
}

SyncLocationInfo *SyncHandler::findLocation(const char* locationName, int slot)
{
	for (SyncLocationInfo *loc = locations[slot]; loc; loc = loc->collision)
		{
		if (loc->name == locationName)
			return loc;
		}

	return NULL;
}
SyncLocationInfo *SyncHandler::addLocation(const char* locationName, SyncObjectInfo *soi)
{
	ASSERT(locationName != NULL);
	ASSERT(strlen(locationName) != 0);

	int slot = JString::hash(locationName, locationHashSize);
	SyncLocationInfo *loc = findLocation(locationName, slot);

	if (!loc)
		{
		loc = new SyncLocationInfo;
		loc->name = locationName;
		loc->soi = soi;
		loc->collision = locations[slot];
		locations[slot] = loc;
		if ((++locationCount % 100) == 0)
			Log::debug("* SyncHandler has found %d unique locations\n",locationCount);
		}

	ASSERT(loc->soi == soi);
	ASSERT(loc->collision != loc);
	
	return loc;
}

void SyncHandler::showLocations(void)
{
	Log::debug("\n\n== SyncHandler has found %d unique locations ==\n",locationCount);
	Log::debug("  _____LocationName_____\t_____SyncObjectName_____\n");

	for (int slot = 0; slot < locationHashSize; slot++)
		for (SyncLocationInfo *loc = locations[slot]; loc; loc = loc->collision)
			Log::debug("  %s\t%s\n", loc->name, loc->soi->name);
}


void SyncHandler::addToThread(SyncThreadInfo* thd, SyncLocationInfo *loc)
{
	// Be sure this soi is only recorded once.

	if (loc->soi->multiple)
		for (int n = 0; n < thd->height; n++)
			if (thd->stack[n]->soi == loc->soi)
				return;

	// add this Sync location to the thread's stack
	ASSERT(thd->height < syncStackSize);
	thd->stack[thd->height++] = loc;

	// Only track stacks of 2 or more

	if (thd->height > 1)
		thd->isStackRecorded =  false;
}

void SyncHandler::delFromThread(SyncThreadInfo* thd, SyncObjectInfo *soi)
{
	// del this Sync location from the thread's stack
	ASSERT(thd->height > 0);
	ASSERT(thd->stack[thd->height] == NULL);
	ASSERT(thd->stack[thd->height - 1] != NULL);

	// Before we take off this location and soi, let's record it.
	// Do it here so that we can avoid recording sub-stacks.

	if (!thd->isStackRecorded)
		addStack(thd);

	// Ususally it will be the last one

	if (thd->stack[thd->height - 1]->soi == soi)
		{
		thd->stack[--thd->height] = NULL;
		return;
		}

	ASSERT(thd->height > 1);

	for (int z = thd->height - 2; z >= 0; z--)
		{
		if (thd->stack[z]->soi == soi)
			{
			for (int a = z; a < thd->height - 1; a++)
				thd->stack[z] = thd->stack[z + 1];

			thd->stack[--thd->height] = NULL;
			return;
			}
		}

	ASSERT (false);  // Did not find the soi being unlocked.
}

LocationStackInfo* SyncHandler::findStack(LocationStackInfo* lsi, int slot)
{
	for (LocationStackInfo* stack = locationStacks[slot]; stack; stack = stack->collision)
		{
		if (stack->hash != lsi->hash)
			continue;

		if (stack->height != lsi->height)
			continue;

		for (int n = 0; n < stack->height; n++)
			if (stack->loc[n] != lsi->loc[n])
				break;

		// The two stacks matched.
		return stack;
		}

	return NULL;
}

void SyncHandler::addStack(SyncThreadInfo* thd)
{
	// Only add stacks of 2 or more
	ASSERT(thd->height > 1);

	LocationStackInfo* locationStk = new LocationStackInfo;
	memset(locationStk, 0, sizeof(LocationStackInfo));
	locationStk->height = thd->height;

	for (int n = 0; n < thd->height; n++)
		{
		ASSERT(thd->stack[n]);
		locationStk->loc[n] = thd->stack[n];
		}

	// Calulate the hash numbers.

	int64 locHash = 0;
	for (int n = 0; n < thd->height; n++)
		locHash += (int) locationStk->loc[n];

	locationStk->hash = (int) ((locHash >> 5) & 0x00000000FFFFFFFF);
	int locSlot = locationStk->hash % stackHashSize;
	
	LocationStackInfo* stack = findStack(locationStk, locSlot);
	if (!stack)
		{
		locationStk->collision = locationStacks[locSlot];
		locationStacks[locSlot] = locationStk;
		if ((++locationStackCount % 100) == 0)
			Log::debug("* SyncHandler has found %d unique location Stacks\n", locationStackCount);
		}
	else
		delete locationStk;

	thd->isStackRecorded = true;
}

void SyncHandler::countLocationStacks(void)
{
	int stackCount = 0;

	for (int slot = 0; slot < stackHashSize; slot++)
		for (LocationStackInfo *lsi = locationStacks[slot]; lsi; lsi = lsi->collision)
			lsi->count = ++stackCount;
}

void SyncHandler::showLocationStacks(void)
{
	Log::debug("\n\n== SyncHandler has found %d unique Location Stacks ==\n", locationStackCount);
	Log::debug("     Count; LocationStack\n");
	int stackCount = 0;

	for (int slot = 0; slot < stackHashSize; slot++)
		for (LocationStackInfo *lsi = locationStacks[slot]; lsi; lsi = lsi->collision)
			{
			int stackHeight = 0;
			stackCount++;
			for (int n = 0; n < lsi->height - 1; n++)
				Log::debug("  %4d-%03d; %s (%s) ->\n", 
				           stackCount, ++stackHeight, 
				           lsi->loc[n]->name, lsi->loc[n]->soi->name);

			Log::debug("  %4d-%03d; %s (%s)\n\n", 
			           stackCount, ++stackHeight, 
			           lsi->loc[lsi->height - 1]->name, 
			           lsi->loc[lsi->height - 1]->soi->name);
			}
}

DeadlockInfo* SyncHandler::findDeadlock(DeadlockInfo* dli, int slot)
{
	for (DeadlockInfo* dead = possibleDeadlocks[slot]; dead; dead = dead->collision)
		{
		if (dead->hash == dli->hash)
			{
			if (   (dead->soi[0] == dli->soi[0])
			    && (dead->soi[1] == dli->soi[1]))
				return dead;

			if (   (dead->soi[0] == dli->soi[1])
			    && (dead->soi[1] == dli->soi[0]))
				return dead;
			}
		}

	return NULL;
}

// Traverse all the known sync objects, and put them in order by the order they occur in the stacks.
void SyncHandler::validate(void)
{
	possibleDeadlockCount = 0;
	for (int n = 0; n < deadlockHashSize; ++n)
		for (DeadlockInfo *dli; (dli = possibleDeadlocks[n]);)
			{
			possibleDeadlocks[n] = dli->collision;
			delete dli;
			}

	int a,b,c,d,e;
	for (a = 0; a < syncObjHashSize; a++)
		for (SyncObjectInfo *soi = syncObjects[a]; soi; soi = soi->collision)
			{
			// Make a list of all SyncObjects that must occur before and after and this.

			SyncObjectInfo *before[1000];
			memset(before, 0, sizeof(before));
			int beforeCount = 0;

			SyncObjectInfo *after[1000];
			memset(after, 0, sizeof(after));
			int afterCount = 0;

			// search each location stack for this soi, make a list of
			// SyncObjects that occur before and after

			for (b = 0; b < stackHashSize; b++)
				for (LocationStackInfo *lsi = locationStacks[b]; lsi; lsi = lsi->collision)
					{
					for (c = 0; c < lsi->height; c++)
						if (soi == lsi->loc[c]->soi)
							{
							for (d = 0; d < c; d++) 
								{
								for (e = 0; e < beforeCount; e++)
									if (before[e] == lsi->loc[d]->soi)
										break;

								if (e == beforeCount)
									before[beforeCount++] = lsi->loc[d]->soi;

								ASSERT(lsi->loc[d]->soi);
								ASSERT(beforeCount < 1000);
								}
							for (d = c + 1; d < lsi->height; d++)
								{
								for (e = 0; e < afterCount; e++)
									if (after[e] == lsi->loc[d]->soi)
										break;

								if (e == afterCount)
									after[afterCount++] = lsi->loc[d]->soi;

								ASSERT(lsi->loc[d]->soi);
								ASSERT(afterCount < 1000);
								}
							}
					}

			// Make sure none of the SyncObjects in before are also in after.

			for (b = 0; b < beforeCount; b++)
				if (soi != before[b])
					for (c = 0; c < afterCount; c++)
						if (before[b] == after[c])
							addPossibleDeadlock(soi, after[c]);
			}
}

void SyncHandler::addPossibleDeadlock(SyncObjectInfo *soi1, SyncObjectInfo *soi2)
{
	ASSERT(soi1 && soi2);
	//ASSERT(soi1 != soi2);

	DeadlockInfo* dli = new DeadlockInfo;
	memset(dli, 0, sizeof(DeadlockInfo));

	dli->soi[0] = soi1;
	dli->soi[1] = soi2;

	// Now calulate the hash number and slot.  
	// This hash algorithm must return the same slot for two SyncObjectInfo *
	// whichever is first or second.

	int64 hash = (int64) soi1 + (int64) soi2;
	dli->hash  = (int) ((hash >> 5) & 0x00000000FFFFFFFF);
	int slot = dli->hash % deadlockHashSize;

	DeadlockInfo *stack = findDeadlock(dli, slot);

	if (!stack)
		{
		dli->collision = possibleDeadlocks[slot];
		possibleDeadlocks[slot] = dli;
		possibleDeadlockCount++;
		Log::debug("  %d - Possible Deadlock;  %s and %s\n", possibleDeadlockCount, soi1->name, soi2->name);
		return;
		}

	delete dli;
}

#define FOUND_FIRST 1
#define FOUND_SECOND 2
#define FOUND_BOTH 3
void SyncHandler::showPossibleDeadlockStacks(void)
{
	int a,b,c;
	int possibleDeadlockCount = 0;
	for (a = 0; a < deadlockHashSize; a++)
		for (DeadlockInfo *dli = possibleDeadlocks[a]; dli; dli = dli->collision)
			possibleDeadlockCount++;

	Log::debug("\n== SyncHandler has found %d possible deadlocks ==\n", possibleDeadlockCount);

	for (a = 0; a < deadlockHashSize; a++)
		for (DeadlockInfo *dli = possibleDeadlocks[a]; dli; dli = dli->collision)
			{
			int stackCount = 0;
			Log::debug("\n=== Possible Deadlock;  %s and %s ===\n    Stacks =", dli->soi[0]->name, dli->soi[1]->name);

			// Reference all call stacks with these two SyncObjects.

			for (b = 0; b < stackHashSize; b++)
				for (LocationStackInfo *lsi = locationStacks[b]; lsi; lsi = lsi->collision)
					{
					// Does this location stack have both SyncObjects?

					int numFound = 0;
					for (c = 0; c < lsi->height; c++)
						{
						if (lsi->loc[c]->soi == dli->soi[0])
							numFound |= FOUND_FIRST;
						else if (lsi->loc[c]->soi == dli->soi[1])
							numFound |= FOUND_SECOND;
						}

					if (numFound == FOUND_BOTH)
						{
						if (stackCount && ((stackCount % 10) == 0))
							Log::debug("\n   ");
						stackCount++;
						Log::debug(" %d", lsi->count);
						}
					}
			Log::debug("\n");
			}
}
#endif
