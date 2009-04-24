#include "Engine.h"
#include "CycleLock.h"
#include "Database.h"
#include "CycleManager.h"
#include "Thread.h"

CycleLock::CycleLock(Database *database)
{
	cycleManager = database->cycleManager;
	syncObject = cycleManager->getSyncObject();
	thread = Thread::getThread("CycleLock::CycleLock");

	// If there already is a cycle manager, let him worry about all this

	if ( (chain = thread->cycleLock) )
		locked = false;
	else
		{
		syncObject->lock(NULL, Shared);
		locked = true;
		thread->cycleLock = this;
		}
}

CycleLock::~CycleLock(void)
{
	if (locked)
		syncObject->unlock(NULL, Shared);

	if (!chain)
		thread->cycleLock = NULL;
}

// Called by somebody down stack about to do a long term wait

CycleLock* CycleLock::unlock(void)
{
	Thread *thread = Thread::getThread("CycleLock::CycleLock");
	CycleLock *cycleLock = thread->cycleLock;
	ASSERT(cycleLock);
	cycleLock->unlockCycle();
	
	return cycleLock;
}

void CycleLock::unlockCycle(void)
{
	if (locked)
		{
		syncObject->unlock(NULL, Shared);
		locked = false;
		}

	if (chain)
		chain->unlockCycle();
}

void CycleLock::lockCycle(void)
{
	if (chain)
		chain->lockCycle();
	else
		{
		syncObject = cycleManager->getSyncObject();
		syncObject->lock(NULL, Shared);
		locked = true;
		}
}

bool CycleLock::isLocked(void)
{
	Thread *thread = Thread::getThread("CycleLock::CycleLock");
	
	if (!thread->cycleLock)
		return false;
	
	return thread->cycleLock->locked;
}
