#include "Engine.h"
#include "CycleManager.h"
#include "Sync.h"
#include "Database.h"
#include "Thread.h"
#include "Threads.h"
#include "RecordVersion.h"
#include "Interlock.h"

static const int CYCLE_SLEEP		= 1000;

CycleManager::CycleManager(Database *db)
{
	database = db;
	thread = NULL;
	records = NULL;
	currentCycle = &cycle1;
	cycle1.setName("CycleManager::cycle1");
	cycle2.setName("CycleManager::cycle2");
}

CycleManager::~CycleManager(void)
{
}

void CycleManager::start(void)
{
	database->threads->start("CycleManager::start", cycleManager, this);
}

void CycleManager::shutdown(void)
{
	if (thread)
		thread->shutdown();
}

void CycleManager::cycleManager(void *arg)
{
	((CycleManager*) arg)->cycleManager();
}

void CycleManager::cycleManager(void)
{
	thread = Thread::getThread("CycleManager::cycleManager");
	
	while (!thread->shutdownInProgress)
		{
		thread->sleep(CYCLE_SLEEP);
		RecordVersion *doomedRecords;
		
		// Pick up detrius registered for delete during cycle
		
		if (records)
			for (;;)
				{
				doomedRecords = records;
				
				if (COMPARE_EXCHANGE_POINTER(&records, doomedRecords, NULL))
					break;
				}
		else
			doomedRecords = NULL;
		
		// Swap cycle clocks to start next cycle
		
		SyncObject *priorCycle = currentCycle;
		currentCycle = (currentCycle == &cycle1) ? &cycle2 : &cycle1;
	
		// Wait for previous cycle to complete
		
		Sync sync(priorCycle, "CycleManager::cycleManager");
		sync.lock(Exclusive);
		sync.unlock();
		
		for (RecordVersion *record; (record = doomedRecords);)
			{
			doomedRecords = record->nextInTrans;
			record->release();
			}
		}
}

void CycleManager::queueForDelete(RecordVersion* record)
{
	for (;;)
		{
		record->nextInTrans = records;
		
		if (COMPARE_EXCHANGE_POINTER(&records, record->nextInTrans, record))
			break;
		}
}
