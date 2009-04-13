#include "Engine.h"
#include "CycleManager.h"
#include "Sync.h"
#include "Database.h"
#include "Thread.h"
#include "Threads.h"
#include "RecordVersion.h"
#include "Interlock.h"

static const int CYCLE_SLEEP		= 1000;

#ifdef _DEBUG
#undef THIS_FILE
static const char THIS_FILE[]=__FILE__;
#endif

CycleManager::CycleManager(Database *db)
{
	database = db;
	thread = NULL;
	records = NULL;
	recordVersions = NULL;
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
		RecordVersion *doomedRecordVersions;
		RecordList *doomedRecords;
		
		// Pick up detrius registered for delete during cycle
		
		if (recordVersions)
			for (;;)
				{
				doomedRecordVersions = recordVersions;
				
				if (COMPARE_EXCHANGE_POINTER(&recordVersions, doomedRecordVersions, NULL))
					break;
				}
		else
			doomedRecordVersions = NULL;
		
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
		
		for (RecordVersion *recordVersion; (recordVersion = doomedRecordVersions);)
			{
			doomedRecordVersions = recordVersion->nextInTrans;
			recordVersion->release();
			}
		
		for (RecordList *recordList; (recordList = doomedRecords);)
			{
			doomedRecords = recordList->next;
			recordList->record->release();
			delete recordList;
			}
		}
}

void CycleManager::queueForDelete(Record* record)
{
	if (record->isVersion())
		{
		RecordVersion *recordVersion = (RecordVersion*) record;
		
		for (;;)
			{
			recordVersion->nextInTrans = recordVersions;
			
			if (COMPARE_EXCHANGE_POINTER(&recordVersions, recordVersion->nextInTrans, recordVersion))
				break;
			}
		}
	else
		{
		RecordList *recordList = new RecordList;
		recordList->record = record;
		
		for (;;)
			{
			recordList->next = records;
			
			if (COMPARE_EXCHANGE_POINTER(&records, recordList->next, recordList))
				break;
			}
		}
}
