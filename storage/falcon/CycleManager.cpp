#include "Engine.h"
#include "CycleManager.h"
#include "Sync.h"
#include "Database.h"
#include "Thread.h"
#include "Threads.h"
#include "RecordVersion.h"
#include "Value.h"
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
	recordPurgatory = NULL;
	recordVersionPurgatory = NULL;
	valuePurgatory = NULL;
	bufferPurgatory = NULL;

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
		ValueList *doomedValues;
		BufferList *doomedBuffers;
		
		// Pick up detrius registered for delete during cycle
		
		if (recordVersionPurgatory)
			for (;;)
				{
				doomedRecordVersions = recordVersionPurgatory;
				
				if (COMPARE_EXCHANGE_POINTER(&recordVersionPurgatory, doomedRecordVersions, NULL))
					break;
				}
		else
			doomedRecordVersions = NULL;
		
		if (recordPurgatory)
			for (;;)
				{
				doomedRecords = recordPurgatory;
				
				if (COMPARE_EXCHANGE_POINTER(&recordPurgatory, doomedRecords, NULL))
					break;
				}
		else
			doomedRecords = NULL;

		if (valuePurgatory)
			for (;;)
				{
				doomedValues = valuePurgatory;
				
				if (COMPARE_EXCHANGE_POINTER(&valuePurgatory, doomedValues, NULL))
					break;
				}
		else
			doomedValues = NULL;
		
		if (bufferPurgatory)
			for (;;)
				{
				doomedBuffers = bufferPurgatory;
				
				if (COMPARE_EXCHANGE_POINTER(&bufferPurgatory, doomedBuffers, NULL))
					break;
				}
		else
			doomedBuffers = NULL;
		
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
			recordVersion->release(REC_HISTORY);
			}

		for (RecordList *recordList; (recordList = doomedRecords);)
			{
			doomedRecords = recordList->next;
			recordList->zombie->release(REC_HISTORY);
			delete recordList;
			}

		for (ValueList *valueList; (valueList = doomedValues);)
			{
			doomedValues = valueList->next;
			delete [] (Value*) valueList->zombie;
			delete valueList;
			}

		for (BufferList *bufferList; (bufferList = doomedBuffers);)
			{
			doomedBuffers = bufferList->next;
			DELETE_RECORD (bufferList->zombie);
			delete bufferList;
			}

		}
}

void CycleManager::queueForDelete(Record* zombie)
{
	if (zombie->isVersion())
		{
		RecordVersion *recordVersion = (RecordVersion*) zombie;
		
		for (;;)
			{
			recordVersion->nextInTrans = recordVersionPurgatory;
			
			if (COMPARE_EXCHANGE_POINTER(&recordVersionPurgatory, recordVersion->nextInTrans, recordVersion))
				break;
			}
		}
	else
		{
		RecordList *recordList = new RecordList;
		recordList->zombie = zombie;
		
		for (;;)
			{
			recordList->next = recordPurgatory;
			
			if (COMPARE_EXCHANGE_POINTER(&recordPurgatory, recordList->next, recordList))
				break;
			}
		}
}
void CycleManager::queueForDelete(Value** zombie)
{
	ValueList *valueList = new ValueList;
	valueList->zombie = zombie;

	for (;;)
		{
		valueList->next = valuePurgatory;
		
		if (COMPARE_EXCHANGE_POINTER(&valuePurgatory, valueList->next, valueList))
			break;
		}
}

void CycleManager::queueForDelete(char* zombie)
{
	BufferList *bufferlist = new BufferList;
	bufferlist->zombie = zombie;

	for (;;)
		{
		bufferlist->next = bufferPurgatory;
		
		if (COMPARE_EXCHANGE_POINTER(&bufferPurgatory, bufferlist->next, bufferlist))
			break;
		}
}
