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

#ifndef _SYNC_HANDLER_H_
#define _SYNC_HANDLER_H_

// The Falcon SyncHandler tracks the usage and call stacks of all SyncObjects.
// It allows you to find potential deadlocks in various call stacks
// Note: it serializes all locks and unlocks, so only use it for analysis.

// Uncomment this to use the SyncHandler
//#define USE_FALCON_SYNC_HANDLER

static const int syncObjHashSize = 101;
static const int locationHashSize = 503;
static const int threadHashSize = 100;
static const int stackHashSize = 1000;
static const int deadlockHashSize = 100;
static const int beforeAfterSize = 60;
static const int syncStackSize = 20;

struct SyncObjectInfo
{
	JString name;
	SyncObjectInfo *prev;
	SyncObjectInfo *next;
	SyncObjectInfo *collision;
	SyncObjectInfo *beforeList[beforeAfterSize];
	SyncObjectInfo *afterList[beforeAfterSize];
	SyncObjectInfo *before;
	SyncObjectInfo *after;
	bool multiple;
};

struct SyncLocationInfo
{
	JString name;
	SyncObjectInfo *soi;
	LockType type;
	SyncLocationInfo *collision;
};

struct SyncThreadInfo
{
	int Id;
	int height;
	bool isStackRecorded;
	SyncLocationInfo *stack[syncStackSize];
	SyncThreadInfo *collision;
};

struct LocationStackInfo
{
	SyncLocationInfo *loc[syncStackSize];
	int height;
	int hash;
	int count;
	bool hasRisingLockTypes;
	LocationStackInfo *collision;
};

struct DeadlockInfo
{
	SyncObjectInfo *soi[2];
	int hash;
	bool isPossible;
	DeadlockInfo *collision;
};

class SyncHandler;
extern "C" 
{
	SyncHandler*	getFalconSyncHandler(void);
	SyncHandler*	findFalconSyncHandler(void);
}

class SyncHandler
{
public:
	SyncHandler(void);
	virtual ~SyncHandler(void);

	void	addLock(SyncObject *syncObj, const char *locationName, LockType type);
	void	delLock(SyncObject *syncObj);
	void	dump(void);

#ifdef USE_FALCON_SYNC_HANDLER

private:
	SyncThreadInfo *	findThread();
	SyncThreadInfo *	findThread(int thdId, int slot);
	SyncThreadInfo *	addThread(int thdId);
	SyncObjectInfo *	findSyncObject(const char* syncObjectName);
	SyncObjectInfo *	findSyncObject(const char* syncObjectName, int slot);
	SyncObjectInfo *	addSyncObject(const char* syncObjectName);
	void				showSyncObjects(void);
	SyncLocationInfo *	findLocation(const char* locationName, LockType type, int slot);
	SyncLocationInfo *	addLocation(const char* locationName, SyncObjectInfo *soi, LockType type);
	void				showLocations(void);
	LocationStackInfo *	findStack(LocationStackInfo* stk, int slot);
	void				addStack(SyncThreadInfo* thd);
	void				showLocationStacks(void);
	void				showLocationStack(int stackNum);
	void				showLocationStack(LocationStackInfo *lsi);
	void				countLocationStacks(void);

	DeadlockInfo *		findDeadlock(DeadlockInfo* dli, int slot);

	void				addToThread(SyncThreadInfo *thd, SyncLocationInfo *loc);
	void				delFromThread(SyncThreadInfo* thd, SyncObjectInfo *soi);

	void				validate(void);
	void				addPossibleDeadlock(SyncObjectInfo *soi1, SyncObjectInfo *soi2);
	void				removePossibleDeadlock(DeadlockInfo* dli);
	void				showPossibleDeadlockStacks(void);
	void				showPossibleDeadlockStack(DeadlockInfo *dli, int showOrder);

	SyncObject			syncObject;

	SyncObjectInfo**	syncObjects;
	SyncLocationInfo**	locations;
	SyncThreadInfo**	threads;
	LocationStackInfo**	locationStacks;
	DeadlockInfo**		possibleDeadlocks;

	int syncObjCount;
	int locationCount;
	int threadCount;
	int locationStackCount;
	int possibleDeadlockCount;
#endif

};

#endif
