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

// Transaction.cpp: implementation of the Transaction class.
//
//////////////////////////////////////////////////////////////////////

#include <memory.h>
#include "Engine.h"
#include "Transaction.h"
#include "Configuration.h"
#include "Database.h"
#include "Dbb.h"
#include "Connection.h"
#include "Table.h"
#include "RecordVersion.h"
#include "SQLError.h"
#include "Sync.h"
#include "PageWriter.h"
#include "Table.h"
#include "Interlock.h"
#include "SavePoint.h"
#include "IOx.h"
#include "DeferredIndex.h"
#include "TransactionManager.h"
#include "SerialLog.h"
#include "SerialLogControl.h"
#include "InfoTable.h"
#include "Thread.h"
#include "Format.h"
#include "LogLock.h"
#include "SRLSavepointRollback.h"
#include "Bitmap.h"
#include "BackLog.h"
#include "Interlock.h"
#include "Error.h"

extern uint		falcon_lock_wait_timeout;

static const char *stateNames [] = {
	"Active",
	"Limbo",
	"Committed",
	"RolledBack",
	"Us",
	"Visible",
	"Invisible",
	"WasActive",
	"Deadlock",
	"Available",
	"Initial",
	"ReadOnly"
	};

static const int INDENT = 5;
static const uint32 MAX_LOW_MEMORY_RECORDS = 1000;

#ifdef _DEBUG
#undef THIS_FILE
static const char THIS_FILE[]=__FILE__;
#endif

//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////

Transaction::Transaction(Connection *cnct, TransId seq)
{
	states = NULL;
	statesAllocated = 0;
	savePoints = NULL;
	freeSavePoints = NULL;
	useCount = 1;
	syncObject.setName("Transaction::syncObject");
	syncActive.setName("Transaction::syncActive");
	syncIndexes.setName("Transaction::syncIndexes");
	syncRecords.setName("Transaction::syncRecords");
	syncSavepoints.setName("Transaction::syncSavepoints");
	firstRecord = NULL;
	lastRecord = NULL;
	dependencies = 0;
	initialize(cnct, seq);
}

void Transaction::initialize(Connection* cnct, TransId seq)
{
	Sync sync(&syncObject, "Transaction::initialize(1)");
	sync.lock(Exclusive);
	ASSERT(savePoints == NULL);
	ASSERT(freeSavePoints == NULL);
	ASSERT(firstRecord == NULL);
	ASSERT(dependencies == 0);
	connection = cnct;
	isolationLevel = connection->isolationLevel;
	mySqlThreadId = connection->mySqlThreadId;
	database = connection->database;
	TransactionManager *transactionManager = database->transactionManager;
	systemTransaction = database->systemConnection == connection;
	transactionId = seq;
	chillPoint = &firstRecord;
	commitTriggers = false;
	hasUpdates = false;
	hasLocks = false;
	writePending = true;
	pendingPageWrites = false;
	waitingFor = NULL;
	curSavePointId = 0;
	deferredIndexes = NULL;
	backloggedRecords = NULL;
	deferredIndexCount = 0;
	xidLength = 0;
	xid = NULL;
	scanIndexCount = 0;
	totalRecordData = 0;
	totalRecords = 0;
	chilledRecords = 0;
	chilledBytes = 0;
	thawedRecords = 0;
	thawedBytes = 0;
	debugThawedRecords = 0;
	debugThawedBytes = 0;
	committedRecords = 0;
	numberStates = 0;
	blockedBy = 0;
	deletedRecords = 0;
	inList = true;
	thread = NULL;
	
	if (seq == 0)
		{
		state = Available;
		systemTransaction = false;
		oldestActive = 0;
		writePending = false;

		return;
		}
	
	for (int n = 0; n < LOCAL_SAVE_POINTS; ++n)
		{
		localSavePoints[n].next = freeSavePoints;
		freeSavePoints = localSavePoints + n;
		}
	
	startTime = database->deltaTime;
	blockingRecord = NULL;
	thread = Thread::getThread("Transaction::initialize");
	syncActive.lock(NULL, Exclusive);
	Transaction *oldest = transactionManager->findOldest();
	oldestActive = (oldest) ? oldest->transactionId : transactionId;
	int count = transactionManager->activeTransactions.count;
	
	if (count > statesAllocated)
		{
		delete [] states;
		statesAllocated = count;
		states = new TransState[statesAllocated];
		}

	if (count)
		for (Transaction *transaction = transactionManager->activeTransactions.first; transaction; transaction = transaction->next)
			if (transaction->isActive() && 
				 !transaction->systemTransaction &&
				 transaction->transactionId < transactionId)
				{
				Sync syncDependency(&transaction->syncObject, "Transaction::initialize(2)");
				syncDependency.lock(Shared);

				if (transaction->isActive() && 
					 !transaction->systemTransaction &&
					 transaction->transactionId < transactionId)
					{
					transaction->addRef();
					INTERLOCKED_INCREMENT(transaction->dependencies);
					TransState *state = states + numberStates;
					state->transaction = transaction;
					state->transactionId = transaction->transactionId;
					state->state = transaction->state;
					++numberStates;
					ASSERT(transaction->transactionId == state->transactionId);
					}
				}

	state = Active;
}

Transaction::~Transaction()
{
	ASSERT(dependencies == 0);
	
	if (state == Active)
		{
		Log::debug("Deleting apparently active transaction %d\n", transactionId);
		ASSERT(false);
		
		if (syncActive.ourExclusiveLock())
			syncActive.unlock();
		}

	if (inList)
		database->transactionManager->removeTransaction(this);

	delete [] states;
	delete [] xid;
	delete backloggedRecords;
	chillPoint = &firstRecord;

	// We modify record list without locking.
	// It is a destructor and if somebody accesses the list
	// at this point, he is already lost.
	for (RecordVersion *record; (record = firstRecord);)
		{
		removeRecordNoLock(record);
		}
	firstRecord = NULL;
	
	releaseSavepoints();
	
	if (deferredIndexes)
		releaseDeferredIndexes();
}

void Transaction::commit()
{
	ASSERT((firstRecord != NULL) || (chillPoint == &firstRecord));

	if (!isActive())
		throw SQLEXCEPTION (RUNTIME_ERROR, "transaction is not active");

	releaseSavepoints();

	if (!hasUpdates)
		{
		commitNoUpdates();
		
		return;
		}

	TransactionManager *transactionManager = database->transactionManager;
	addRef();
	Log::log(LogXARecovery, "%d: Commit transaction %d\n", database->deltaTime, transactionId);

	if (state == Active)
		{
		Sync sync(&syncIndexes, "Transaction::commit(1)");
		sync.lock(Shared);
		
		for (DeferredIndex *deferredIndex= deferredIndexes; deferredIndex;  
			 deferredIndex = deferredIndex->nextInTransaction)
			if (deferredIndex->index)
				database->dbb->logIndexUpdates(deferredIndex);
		
		sync.unlock();
		database->dbb->logUpdatedRecords(this, firstRecord);

		if (pendingPageWrites)
			database->pageWriter->waitForWrites(this);
		}
			
	++transactionManager->committed;

	if (hasLocks)
		releaseRecordLocks();

	database->serialLog->preCommit(this);
	syncActive.unlock();

	

	Sync syncRec(&syncRecords,"Transaction::commit(1.5)");
	syncRec.lock(Shared);
	for (RecordVersion *record = firstRecord; record; record = record->nextInTrans)
	{
		Table * table = record->format->table;

		if (!record->isSuperceded() && record->state != recLock)
			{
			table->updateRecord (record);
			if (commitTriggers)
				table->postCommit (this, record);
			}

		if (!record->getPriorVersion())
			++table->cardinality;
		if (record->state == recDeleted && table->cardinality > 0)
			--table->cardinality;
	}
	syncRec.unlock();

	releaseDependencies();
	database->flushInversion(this);

	// Transfer transaction from active list to committed list, set committed state

	Sync syncCommitted(&transactionManager->committedTransactions.syncObject, "Transaction::commit(2)");
	Sync syncActiveTransactions(&transactionManager->activeTransactions.syncObject, "Transaction::commit(3)");

	syncActiveTransactions.lock(Exclusive);
	syncCommitted.lock(Exclusive);

	transactionManager->activeTransactions.remove(this);
	transactionManager->committedTransactions.append(this);
	state = Committed;

	syncCommitted.unlock();
	syncActiveTransactions.unlock();

	database->commit(this);

	delete [] xid;
	xid = NULL;
	xidLength = 0;
	
	// If there's no reason to stick around, just go away
	
	if ((dependencies == 0) && !writePending)
		commitRecords();

	connection = NULL;
	
	// Add ourselves to the list of lingering committed transactions
	
	release();
}


void Transaction::commitNoUpdates(void)
{
	TransactionManager *transactionManager = database->transactionManager;
	addRef();
	ASSERT(!deferredIndexes);
	++transactionManager->committed;
	
	if (deferredIndexes)
		releaseDeferredIndexes();

	if (hasLocks)
		releaseRecordLocks();

	Sync syncActiveTransactions(&transactionManager->activeTransactions.syncObject, "Transaction::commitNoUpdates(2)");
	syncActiveTransactions.lock(Shared);
	releaseDependencies();

	if (xid)
		{
		delete [] xid;
		xid = NULL;
		xidLength = 0;
		}
	
	Sync sync(&syncObject, "Transaction::commitNoUpdates(3)");
	sync.lock(Exclusive);
	
	if (dependencies)
		transactionManager->expungeTransaction(this);
	
	// If there's no reason to stick around, just go away
	
	connection = NULL;
	transactionId = 0;
	writePending = false;
	syncActiveTransactions.unlock();
	syncActive.unlock();
	release();
	state = Available;
}

void Transaction::rollback()
{
	RecordVersion *stack = NULL;
	RecordVersion *record;

	if (!isActive())
		throw SQLEXCEPTION (RUNTIME_ERROR, "transaction is not active");

	if (deferredIndexes)
		releaseDeferredIndexes();
		
	releaseSavepoints();
	TransactionManager *transactionManager = database->transactionManager;
	Transaction *rollbackTransaction = transactionManager->rolledBackTransaction;
	chillPoint = &firstRecord;
	totalRecordData = 0;
	totalRecords = 0;

	// Rollback pending record versions from newest to oldest in case
	// there are multiple record versions on a prior record chain

	Sync syncRec(&syncRecords, "Transaction::rollback(1.5)");
	syncRec.lock(Exclusive);

	while (firstRecord)
		{
		record = firstRecord;
		firstRecord = record->nextInTrans;
		record->prevInTrans = NULL;
		record->nextInTrans = stack;
		stack = record;
		}
		
	lastRecord = NULL;

	while (stack)
		{
		record = stack;
		stack = record->nextInTrans;
		record->nextInTrans = NULL;

		if (record->state == recLock)
			record->format->table->unlockRecord(record, false);
		else
			record->rollback(this);
		
		record->transaction = rollbackTransaction;
		record->release();
		}
	firstRecord = NULL;
	syncRec.unlock();

	for (SavePoint *savePoint = savePoints; savePoint; savePoint = savePoint->next)
		if (savePoint->backloggedRecords)
			database->backLog->rollbackRecords(savePoint->backloggedRecords, this);

	if (backloggedRecords)
		database->backLog->rollbackRecords(backloggedRecords, this);

	ASSERT(writePending);
	state = RolledBack;
	writePending = false;
	releaseDependencies();
	syncActive.unlock();
	
	if (hasUpdates)
		database->serialLog->preCommit(this);
		
	database->rollback(this);
	
	if (xid)
		{
		delete [] xid;
		xid = NULL;
		xidLength = 0;
		}
	
	Sync syncActiveTransactions (&transactionManager->activeTransactions.syncObject, "Transaction::rollback(2)");
	syncActiveTransactions.lock (Exclusive);
	++transactionManager->rolledBack;
	
	while (dependencies)
		transactionManager->expungeTransaction(this);
		
	ASSERT(dependencies == 0);
	inList = false;
	transactionManager->activeTransactions.remove(this);
	syncActiveTransactions.unlock();
	release();
}


void Transaction::expungeTransaction(Transaction * transaction)
{
	ASSERT(states != NULL || numberStates == 0);
	
	for (TransState *s = states, *end = s + numberStates; s < end; ++s)
		if (s->transaction == transaction)
			{
			if (COMPARE_EXCHANGE_POINTER(&s->transaction, transaction, NULL))
				transaction->releaseDependency();

			break;
			}
}

void Transaction::prepare(int xidLen, const UCHAR *xidPtr)
{
	if (state != Active)
		throw SQLEXCEPTION (RUNTIME_ERROR, "transaction is not active");

	Log::log(LogXARecovery, "Prepare transaction %d: xidLen = %d\n", transactionId, xidLen);
	releaseSavepoints();

	xidLength = xidLen;

	if (xidLength)
		{
		xid = new UCHAR[xidLength];
		memcpy(xid, xidPtr, xidLength);
		}
		
	database->pageWriter->waitForWrites(this);
	state = Limbo;
	database->dbb->prepareTransaction(transactionId, xidLength, xid);

	Sync sync(&syncIndexes, "Transaction::prepare");
	sync.lock(Shared);
	
	for (DeferredIndex *deferredIndex= deferredIndexes; deferredIndex;  
		deferredIndex = deferredIndex->nextInTransaction)
		if (deferredIndex->index)
			database->dbb->logIndexUpdates(deferredIndex);
	
	sync.unlock();
	database->dbb->logUpdatedRecords(this, firstRecord);

	if (pendingPageWrites)
		database->pageWriter->waitForWrites(this);

	if (hasLocks)
		releaseRecordLocks();
}

void Transaction::chillRecords()
{
#ifdef DEBUG_BACKLOG
	database->setLowMemory();
#endif

	// chillPoint points to a pointer to the first non-chilled record. If any
	// records have been thawed, then reset chillPoint.
	
	if (thawedRecords)
		chillPoint = &firstRecord;
		
	uint32 chilledBefore = chilledRecords;
	uint64 totalDataBefore = totalRecordData;
	database->dbb->logUpdatedRecords(this, *chillPoint, true);
	
	for (SavePoint *savePoint = savePoints; savePoint; savePoint = savePoint->next)
		if (savePoint->id != curSavePointId)
			savePoint->setIncludedSavepoint(curSavePointId);
	
	if (database->lowMemory)
		backlogRecords();

	Log::log(LogInfo, "%d: Record chill: transaction %ld, %ld records, %ld bytes\n", database->deltaTime, transactionId, chilledRecords-chilledBefore,
				(uint32)(totalDataBefore-totalRecordData), committedRecords);
}

int Transaction::thaw(RecordVersion * record)
{
	// Nothing to do if record is no longer chilled
	
	if (record->state != recChilled)
		return record->size;
		
	// Get pointer to record data in serial log

	SerialLogControl control(database->dbb->serialLog);
	
	// Thaw the record then update the total record data bytes for this transaction
	
	ASSERT(record->transactionId == transactionId);
	bool thawed;
	int bytesRestored = control.updateRecords.thaw(record, &thawed);
	
	if (bytesRestored > 0 && thawed)
		{
		totalRecordData += bytesRestored;
		thawedRecords++;
		thawedBytes += bytesRestored;
		debugThawedRecords++;
		debugThawedBytes += bytesRestored;
		}

	if (debugThawedBytes >= database->configuration->recordChillThreshold)
		{
		Log::log(LogInfo, "%d: Record thaw: transaction %ld, %ld records, %ld bytes\n", 
				 database->deltaTime, transactionId, debugThawedRecords, debugThawedBytes);
		debugThawedRecords = 0;
		debugThawedBytes = 0;
		}
	
	return bytesRestored;
}

void Transaction::thaw(DeferredIndex * deferredIndex)
{
	SerialLogControl control(database->dbb->serialLog);
	control.updateIndex.thaw(deferredIndex);
}

void Transaction::addRecord(RecordVersion * record)
{
	ASSERT(record->recordNumber >= 0);
	hasUpdates = true;
	
	if (record->state == recLock)
		hasLocks = true;
	else if (record->state == recDeleted)
		++deletedRecords;
		
	totalRecordData += record->getEncodedSize();
	++totalRecords;
	
	if (totalRecordData > database->configuration->recordChillThreshold)
		{
		// Chill all records except the current record, which may be part of an update or insert

		UCHAR saveState = record->state;
		
		if (record->state != recLock && record->state != recChilled)
			record->state = recNoChill;
			
		chillRecords();

		if (record->state == recNoChill)
			record->state = saveState;
		}

	record->addRef();
	
	Sync syncRec(&syncRecords,"Transaction::addRecord");
	syncRec.lock(Exclusive);
	if ( (record->prevInTrans = lastRecord) )
		lastRecord->nextInTrans = record;
	else
		firstRecord = record;
		
	record->nextInTrans = NULL;
	lastRecord = record;
	syncRec.unlock();
	
	if (database->lowMemory && deletedRecords > MAX_LOW_MEMORY_RECORDS)
		backlogRecords();
}

void Transaction::removeRecord(RecordVersion *record)
{
	Sync syncRec(&syncRecords,"Transaction::removeRecord");
	syncRec.lock(Exclusive);
	removeRecordNoLock(record);
}
void Transaction::removeRecordNoLock(RecordVersion *record)
{
	RecordVersion **ptr;

	if (record->nextInTrans)
		record->nextInTrans->prevInTrans = record->prevInTrans;
	else
		{
		ASSERT(lastRecord == record);
		lastRecord = record->prevInTrans;
		}

	if (record->prevInTrans)
		ptr = &record->prevInTrans->nextInTrans;
	else
		{
		ASSERT(firstRecord == record);
		ptr = &firstRecord;
		}

	*ptr = record->nextInTrans;
	record->prevInTrans = NULL;
	record->nextInTrans = NULL;
	record->transaction = NULL;

	for (SavePoint *savePoint = savePoints; savePoint; savePoint = savePoint->next)
		if (savePoint->records == &record->nextInTrans)
			savePoint->records = ptr;

	if (chillPoint == &record->nextInTrans)
		chillPoint = ptr;

	// Adjust total record data count
	
	if (record->state != recChilled)
		{
		uint32 size = record->getEncodedSize();
		
		if (totalRecordData >= size)
			totalRecordData -= size;
		}

	if (record->state == recDeleted && deletedRecords > 0)
		--deletedRecords;
	
	record->release();
}

/***
@brief		Determine if changes by another transaction are visible to this.
@details	This function is called for Consistent-Read transactions to determine
			if the sent trans was committed before this transaction started.  If not,
			it is invisible to this transaction.
***/

bool Transaction::visible(Transaction * transaction, TransId transId, int forWhat)
{
	// If the transaction is NULL, it is long gone and therefore committed

	if (!transaction)
		return true;

	// If we're the transaction in question, consider us committed

	if (transId == transactionId)
		return true;

	// If we're the system transaction, just use the state of the other transaction

	if (database->systemConnection->transaction == this)
		return transaction->state == Committed;

	// If the other transaction is not yet committed, the trans is not visible.

	if (transaction->state != Committed)
		return false;

	// The other transaction is committed.  
	// If this is READ_COMMITTED, it is visible.

	if (   IS_READ_COMMITTED(isolationLevel)
		|| (   IS_WRITE_COMMITTED(isolationLevel)
		    && (forWhat == FOR_WRITING)))
		return true;

	// This is REPEATABLE_READ
	ASSERT (IS_REPEATABLE_READ(isolationLevel));

	// If the transaction started after we did, consider the transaction active

	if (transId > transactionId)
		return false;

	// If the transaction was active when we started, use it's state at that point

	for (int n = 0; n < numberStates; ++n)
		if (states [n].transactionId == transId)
			return false;

	return true;
}

/***
@brief		Determine if there is a need to lock this record for update.
***/

bool Transaction::needToLock(Record* record)
{
	// Find the first visible record version

	Sync syncPrior(record->getSyncPrior(), "Transaction::needToLock");
	syncPrior.lock(Shared);

	for (Record* candidate = record; 
		 candidate != NULL;
		 candidate = candidate->getPriorVersion())
		{
		Transaction *transaction = candidate->getTransaction();
		TransId transId = candidate->getTransactionId();

		if (visible(transaction, transId, FOR_WRITING))
			if (candidate->state == recDeleted)
				if (!transaction || transaction->state == Committed)
					return false; // Committed and deleted
				else
					return true; // Just in case this rolls back.
			else
				return true;
		}

	return false;
}

void Transaction::releaseDependencies()
{
	if (!numberStates)
		return;

	for (TransState *state = states, *end = states + numberStates; state < end; ++state)
		{
		Transaction *transaction = state->transaction;

		if (transaction)
			{
			if (transaction->transactionId != state->transactionId)
				{
				Transaction *transaction = database->transactionManager->findTransaction(state->transactionId);
				ASSERT(transaction == NULL);
				}

			if (COMPARE_EXCHANGE_POINTER(&state->transaction, transaction, NULL))
				{
				ASSERT(transaction->transactionId == state->transactionId || transaction->transactionId == 0);
				ASSERT(transaction->state != Initializing);
				transaction->releaseDependency();
				}
			}
		}
}

/*
 *  Transaction is fully mature and about to go away.
 *  Fully commit all records
 */

void Transaction::commitRecords()
{
	Sync syncRec(&syncRecords,"Transaction::commitRecords");
	syncRec.lock(Exclusive);
	for (RecordVersion *recordList; (recordList = firstRecord);)
		{
		if (recordList && COMPARE_EXCHANGE_POINTER(&firstRecord, recordList, NULL))
			{
			chillPoint = &firstRecord;
			lastRecord = NULL;

			for (RecordVersion *record; (record = recordList);)
				{
				ASSERT (record->useCount > 0);
				recordList = record->nextInTrans;
				record->nextInTrans = NULL;
				record->prevInTrans = NULL;
				record->commit();
				record->release();
				committedRecords++;
				}
			
			return;
			}
			
		Log::debug("Transaction::commitRecords failed\n");
		}
}

/***
@brief		Get the relative state between this transaction and 
			the transaction associated with a record version.
***/

State Transaction::getRelativeState(Record* record, uint32 flags)
{
	blockingRecord = record;
	State state = getRelativeState(record->getTransaction(), record->getTransactionId(), flags);
	blockingRecord = NULL;

	return state;
}

/***
@brief		Get the relative state between this transaction and another.
***/

State Transaction::getRelativeState(Transaction *transaction, TransId transId, uint32 flags)
{
	if (transactionId == transId)
		return Us;

	// A record may still have the transId even after the trans itself has been deleted.
	
	if (!transaction)
		{
		// All calls to getRelativeState are for the purpose of writing.
		// So only ConsistentRead can get CommittedInvisible.

		if (IS_CONSISTENT_READ(isolationLevel))
			{
			// If the transaction is no longer around, and the record is,
			// then it must be committed.

			if (transactionId < transId)
				return CommittedInvisible;

			// Be sure it was not active when we started.

			for (int n = 0; n < numberStates; ++n)
				if (states [n].transactionId == transId)
					return CommittedInvisible;
			}

		return CommittedVisible;
		}

	if (transaction->isActive())
		{
		if (flags & DO_NOT_WAIT)
			return Active;

		bool isDeadlock;
		waitForTransaction(transaction, 0 , &isDeadlock);

		if (isDeadlock)
			return Deadlock;

		return WasActive;			// caller will need to re-fetch
		}

	if (transaction->state == Committed)
		{
		// Return CommittedVisible if the other trans has a lower TransId and 
		// it was committed when we started.
		
		if (visible (transaction, transId, FOR_WRITING))
			return CommittedVisible;

		return CommittedInvisible;
		}

	return (State) transaction->state;
}

void Transaction::dropTable(Table* table)
{
	releaseDeferredIndexes(table);

	Sync syncRec(&syncRecords,"Transaction::dropTable(2)");
	syncRec.lock(Exclusive);
	for (RecordVersion **ptr = &firstRecord, *rec; (rec = *ptr);)
		if (rec->format->table == table)
			removeRecord(rec);
		else
			ptr = &rec->nextInTrans;
}

void Transaction::truncateTable(Table* table)
{
	releaseDeferredIndexes(table);
	Sync syncRec(&syncRecords,"Transaction::truncateTable(2)");
	syncRec.lock(Exclusive);
	for (RecordVersion **ptr = &firstRecord, *rec; (rec = *ptr);)
		if (rec->format->table == table)
			removeRecord(rec);
		else
			ptr = &rec->nextInTrans;
}

bool Transaction::hasRecords(Table* table)
{
	Sync syncRec(&syncRecords, "Transaction::hasRecords");
	syncRec.lock(Shared);
	for (RecordVersion *rec = firstRecord; rec; rec = rec->nextInTrans)
		if (rec->format->table == table)
			return true;
	
	return false;
}

void Transaction::writeComplete(void)
{
	ASSERT(writePending);
	ASSERT(state == Committed);
	releaseDeferredIndexes();
	
	if (dependencies == 0)
		commitRecords();

	writePending = false;
}

bool Transaction::waitForTransaction(TransId transId)
{
	bool deadlock;
	State state = waitForTransaction(NULL, transId, &deadlock);
	return (deadlock || state == Committed || state == Available);

}

// Wait for transaction, unless it would lead to deadlock.
// Returns the state of transation.
//
// Note:
// Deadlock check could use locking, because  there are potentially concurrent
// threads checking and modifying the waitFor list.
// Instead, it implements a fancy lock-free algorithm  that works reliably only
// with full memory barriers. Thus "volatile"-specifier and COMPARE_EXCHANGE
// are used  when traversing and modifying waitFor list. Maybe it is better to
// use inline assembly or intrinsics to generate memory barrier instead of 
// volatile. 

State Transaction::waitForTransaction(Transaction *transaction, TransId transId,
										bool *deadlock)
{


	*deadlock = false;
	State state;

	if(transaction)
		transaction->addRef();

	TransactionManager *transactionManager = database->transactionManager;
	Sync syncActiveTransactions(&transactionManager->activeTransactions.syncObject,
		"Transaction::waitForTransaction(1)");
	syncActiveTransactions.lock(Shared);

	if (!transaction)
		{
		// transaction parameter is not given, find transaction using its ID.
		for (transaction = transactionManager->activeTransactions.first; transaction;
			 transaction = transaction->next)
			{
			if (transaction->transactionId == transId)
				{
				transaction->addRef();
				break;
				}
			}
		}

	if (!transaction)
		return Committed;

	if (transaction->state == Available || transaction->state == Committed)
	{
		state = (State)transaction->state;
		transaction->release();
		return state;
	}


	if (!COMPARE_EXCHANGE_POINTER(&waitingFor, NULL, transaction))
		FATAL("waitingFor was not NULL");

	volatile Transaction *trans;
	for (trans = transaction->waitingFor; trans; trans = trans->waitingFor)
		if (trans == this)
			{
			*deadlock = true;
			break;
			}

	if (!(*deadlock))
		{
		try
			{
			syncActiveTransactions.unlock();
			transaction->waitForTransaction();
			}
		catch(...)
			{
			if (!COMPARE_EXCHANGE_POINTER(&waitingFor, transaction, NULL))
				FATAL("waitingFor was not %p",transaction);
			throw;
			}
		}

	if (!COMPARE_EXCHANGE_POINTER(&waitingFor, transaction, NULL))
		FATAL("waitingFor was not %p",transaction);

	state = (State)transaction->state;
	transaction->release();

	return state;
}

void Transaction::waitForTransaction()
{
	/***
	Thread *exclusiveThread = syncActive.getExclusiveThread();
	
	if (exclusiveThread)
		{
		char buffer[1024];
		connection->getCurrentStatement(buffer, sizeof(buffer));
		Log::debug("Blocking on %d: %s\n", exclusiveThread->threadId, buffer);
		}
	***/
	
	Sync sync(&syncActive, "Transaction::waitForTransaction(2)");
	sync.lock(Shared, falcon_lock_wait_timeout * 1000);
}

void Transaction::addRef()
{
	INTERLOCKED_INCREMENT(useCount);
}

int Transaction::release()
{
	int count = INTERLOCKED_DECREMENT(useCount);

	if (count == 0)
		delete this;

	return count;
}

int Transaction::createSavepoint()
{
	Sync sync(&syncSavepoints, "Transaction::createSavepoint");
	SavePoint *savePoint;
	
	ASSERT((savePoints || freeSavePoints) ? (savePoints != freeSavePoints) : true);
	
	// System transactions require an exclusive lock for concurrent access
	
	if (systemTransaction)
		sync.lock(Exclusive);
	
	if ( (savePoint = freeSavePoints) )
		freeSavePoints = savePoint->next;
	else
		savePoint = new SavePoint;
	
	savePoint->records = (lastRecord) ? &lastRecord->nextInTrans : &firstRecord;
	savePoint->id = ++curSavePointId;
	savePoint->next = savePoints;
	savePoint->savepoints = NULL;
	savePoint->backloggedRecords = NULL;
	savePoints = savePoint;
	ASSERT(savePoint->next != savePoint);
 	
	return savePoint->id;
}

void Transaction::releaseSavepoint(int savePointId)
{
	//validateRecords();
	Sync sync(&syncSavepoints, "Transaction::releaseSavepoint");

	// System transactions require an exclusive lock for concurrent access

	if (systemTransaction)
		sync.lock(Exclusive);
	
	for (SavePoint **ptr = &savePoints, *savePoint; (savePoint = *ptr); ptr = &savePoint->next)
		if (savePoint->id == savePointId)
			{
			int nextLowerSavePointId = (savePoint->next) ? savePoint->next->id : 0;
			*ptr = savePoint->next;
			
			// If we have backed logged records, merge them in to the previous savepoint or the transaction itself.
			
			if (savePoint->backloggedRecords)
				{
				SavePoint *nextSavePoint = savePoint->next;
				
				if (nextSavePoint)
					{
					if (nextSavePoint->backloggedRecords)
						nextSavePoint->backloggedRecords->orBitmap(savePoint->backloggedRecords);
					else
						nextSavePoint->backloggedRecords = savePoint->backloggedRecords;
					
					}
				else
					{
					if (backloggedRecords)
						backloggedRecords->orBitmap(savePoint->backloggedRecords);
					else
						backloggedRecords = savePoint->backloggedRecords;
					}
					
				savePoint->backloggedRecords = NULL;
				}
					
			if (savePoint->savepoints)
				savePoint->clear();

			// commit pending record versions to the next pending savepoint
			
			for (RecordVersion *record = *savePoint->records; record && record->savePointId == savePointId; record = record->nextInTrans)
				{
				record->savePointId = nextLowerSavePointId;
				record->scavenge(transactionId, nextLowerSavePointId);
				}

			savePoint->next = freeSavePoints;
			freeSavePoints = savePoint;
			ASSERT((savePoints || freeSavePoints) ? (savePoints != freeSavePoints) : true);
			//validateRecords();

			return;
			}

	//throw SQLError(RUNTIME_ERROR, "invalid savepoint");
}

void Transaction::releaseSavepoints(void)
{
	Sync sync(&syncSavepoints, "Transaction::releaseSavepoints");
	SavePoint *savePoint;
	
	// System transactions require an exclusive lock for concurrent access

	if (systemTransaction)
		sync.lock(Exclusive);
	
	
	while ( (savePoint = savePoints) )
		{
		savePoints = savePoint->next;
		
		if (savePoint->savepoints)
			savePoint->clear();
			
		if (savePoint < localSavePoints || savePoint >= localSavePoints + LOCAL_SAVE_POINTS)
			delete savePoint;
		}

	while ( (savePoint = freeSavePoints) )
		{
		freeSavePoints = savePoint->next;
		
		if (savePoint < localSavePoints || savePoint >= localSavePoints + LOCAL_SAVE_POINTS)
			delete savePoint;
		}
}

void Transaction::rollbackSavepoint(int savePointId)
{
	//validateRecords();
	Sync sync(&syncSavepoints, "Transaction::rollbackSavepoints");
	SavePoint *savePoint;

	// System transactions require an exclusive lock for concurrent access

	if (systemTransaction)
		sync.lock(Exclusive);

	// Be sure the target savepoint is valid before rolling them back.
	
	for (savePoint = savePoints; savePoint; savePoint = savePoint->next)
		if (savePoint->id <= savePointId)
			break;
			
	if ((savePoint) && (savePoint->id != savePointId))
		throw SQLError(RUNTIME_ERROR, "invalid savepoint");

	if (chilledRecords)
		{
		database->serialLog->logControl->savepointRollback.append(transactionId, savePointId);
		
		if (savePoint->savepoints)
			for (int n = 0; (n = savePoint->savepoints->nextSet(n)) >= 0; ++n)
				database->serialLog->logControl->savepointRollback.append(transactionId, n);
		}				

	savePoint = savePoints;
	

	while (savePoint)
		{
		//validateRecords();
		
		if (savePoint->id < savePointId)
			break;

		// Purge out records from this savepoint
		Sync syncRec(&syncRecords,"Transaction::rollbackSavepoint(2)");
		syncRec.lock(Exclusive);

		RecordVersion *record = *savePoint->records;
		RecordVersion *stack = NULL;

		if (record)
			{
			if ( (lastRecord = record->prevInTrans) )
				lastRecord->nextInTrans = NULL;
			else
				firstRecord = NULL;
			}
	
		while (record)
			{
			if (chillPoint == &record->nextInTrans)
				chillPoint = savePoint->records;

			RecordVersion *rec = record;
			record = rec->nextInTrans;
			rec->prevInTrans = NULL;
			rec->nextInTrans = stack;
			stack = rec;
			
			if (rec->state == recDeleted)
				--deletedRecords;
			}

		while (stack)
			{
			RecordVersion *rec = stack;
			stack = rec->nextInTrans;
			rec->nextInTrans = NULL;
			rec->rollback(this);
#ifdef CHECK_RECORD_ACTIVITY
			rec->active = false;
#endif
			rec->transaction = NULL;
			rec->release();
			}
		syncRec.unlock();

		// Handle any backlogged records
		
		if (savePoint->backloggedRecords)
			database->backLog->rollbackRecords(savePoint->backloggedRecords, this);
				
		// Move skipped savepoints object to the free list
		// Leave the target savepoint empty, but connected to the transaction.
		
		if (savePoint->id > savePointId)
			{
			savePoints = savePoint->next;
			savePoint->next = freeSavePoints;
			freeSavePoints = savePoint;
			savePoint = savePoints;
			}
		else
			savePoint = savePoint->next;

		//validateRecords();
		}
}

void Transaction::add(DeferredIndex* deferredIndex)
{
	Sync sync(&syncIndexes, "Transaction::add");
	sync.lock(Exclusive);
	deferredIndex->nextInTransaction = deferredIndexes;
	deferredIndexes = deferredIndex;
	deferredIndexCount++;
}

bool Transaction::isXidEqual(int testLength, const UCHAR* test)
{
	if (testLength != xidLength)
		return false;
	
	return memcmp(xid, test, xidLength) == 0;
}

void Transaction::releaseRecordLocks(void)
{
	RecordVersion **ptr;
	RecordVersion *record;

	Sync syncRec(&syncRecords,"Transaction::releaseRecordLocks");
	syncRec.lock(Exclusive);
	for (ptr = &firstRecord; (record = *ptr);)
		if (record->state == recLock)
			{
			record->format->table->unlockRecord(record, false);
			removeRecord(record);
			}
		else
			ptr = &record->nextInTrans;
	syncRec.unlock();
}

void Transaction::print(void)
{
	Log::debug("  %p Id %d, state %d, updates %d, wrtPend %d, states %d, dependencies %d, records %d\n",
			this, transactionId, state, hasUpdates, writePending, 
			numberStates, dependencies, firstRecord != NULL);
}

void Transaction::printBlocking(int level)
{
	int locks = 0;
	int updates = 0;
	int inserts = 0;
	int deletes = 0;
	RecordVersion *record;

	Sync syncRec(&syncRecords,"Transaction::printBlocking");
	syncRec.lock(Shared);
	for (record = firstRecord; record; record = record->nextInTrans)
		if (record->state == recLock)
			++locks;
		else if (!record->hasRecord())
			++deletes;
		else if (record->getPriorVersion())
			++updates;
		else
			++inserts;

	Log::debug ("%*s Trans %d, thread %d, locks %d, inserts %d, deleted %d, updates %d\n", 
				level * INDENT, "", transactionId,
				thread->threadId, locks, inserts, deletes, updates);

	++level;

	if (blockingRecord)
		{
		Table *table = blockingRecord->format->table;
		Log::debug("%*s Blocking on %s.%s record %d\n",
				   level * INDENT, "",
				   table->schemaName, table->name, 
				   blockingRecord->recordNumber);
		}

	for (record = firstRecord; record; record = record->nextInTrans)
		{
		const char *what;

		if (record->state == recLock)
			what = "locked";
		else if (!record->hasRecord())
			what = "deleted";
		else if (record->getPriorVersion())
			what = "updated";
		else
			what = "inserted";

		Table *table = record->format->table;
		
		Log::debug("%*s Record %s.%s number %d %s\n",
				   level * INDENT, "",
				   table->schemaName,
				   table->name, 
				   record->recordNumber,
				   what);
		}
	syncRec.unlock();
	database->transactionManager->printBlocking(this, level);
}

void Transaction::getInfo(InfoTable* infoTable)
{
	if (!(state == Available && dependencies == 0))
		{
		int n = 0;
		infoTable->putString(n++, stateNames[state]);
		infoTable->putInt(n++, mySqlThreadId);
		infoTable->putInt(n++, transactionId);
		infoTable->putInt(n++, hasUpdates);
		infoTable->putInt(n++, writePending);
		infoTable->putInt(n++, dependencies);
		infoTable->putInt(n++, oldestActive);
		infoTable->putInt(n++, firstRecord != NULL);
		infoTable->putInt(n++, (waitingFor) ? waitingFor->transactionId : 0);
		
		char buffer[512];
		
		if (connection)
			connection->getCurrentStatement(buffer, sizeof(buffer));
		else
			buffer[0] = 0;

		infoTable->putString(n++, buffer);
		infoTable->putRecord();
		}
}

void Transaction::releaseDependency(void)
{
	ASSERT(useCount >= 2);
	ASSERT(dependencies > 0);
	INTERLOCKED_DECREMENT(dependencies);

	if ((dependencies == 0) && !writePending && firstRecord)
		commitRecords();
	releaseCommittedTransaction();
}

void Transaction::fullyCommitted(void)
{
	ASSERT(inList);

	if (useCount < 2)
		Log::debug("Transaction::fullyCommitted: funny use count\n");

	writeComplete();
	releaseCommittedTransaction();
}

void Transaction::releaseCommittedTransaction(void)
{
	release();

	if ((useCount == 1) && (state == Committed) && (dependencies == 0) && !writePending)
		if (COMPARE_EXCHANGE(&inList, (INTERLOCK_TYPE) true, (INTERLOCK_TYPE) false))
			database->transactionManager->removeCommittedTransaction(this);
}

void Transaction::validateDependencies(bool noDependencies)
{
	for (TransState *state = states, *end = states + numberStates; state < end; ++state)
		if (state->transaction)
			{
			ASSERT(!noDependencies);
			ASSERT(state->transaction->transactionId == state->transactionId);
			}
}

void Transaction::printBlockage(void)
{
	TransactionManager *transactionManager = database->transactionManager;
	LogLock logLock;
	Sync sync (&transactionManager->activeTransactions.syncObject, "Transaction::printBlockage");
	sync.lock (Shared);
	printBlocking(0);
}

void Transaction::releaseDeferredIndexes(void)
{
	Sync sync(&syncIndexes, "Transaction::releaseDeferredIndexes");
	sync.lock(Exclusive);
	for (DeferredIndex *deferredIndex; (deferredIndex = deferredIndexes);)
		{
		ASSERT(deferredIndex->transaction == this);
		deferredIndexes = deferredIndex->nextInTransaction;
		deferredIndex->detachTransaction();
		deferredIndexCount--;
		}
}

void Transaction::releaseDeferredIndexes(Table* table)
{
	for (DeferredIndex **ptr = &deferredIndexes, *deferredIndex; (deferredIndex = *ptr);)
		{
		if (deferredIndex->index && (deferredIndex->index->table == table))
			{
			*ptr = deferredIndex->nextInTransaction;
			deferredIndex->detachTransaction();
			--deferredIndexCount;
			}
		else
			ptr = &deferredIndex->next;
		}
}

void Transaction::backlogRecords(void)
{
	SavePoint *savePoint = savePoints;
	
	for (RecordVersion *record = lastRecord, *prior; record; record = prior)
		{
		prior = record->prevInTrans;
		
		if (!record->hasRecord())
			{
			if (savePoints)
				for (; savePoint && record->savePointId < savePoint->id; savePoint = savePoint->next)
					;	
									
			if (savePoint)
				savePoint->backlogRecord(record);
			else
				{
				if (!backloggedRecords)
					backloggedRecords = new Bitmap;
				
				int32 backlogId = record->format->table->backlogRecord(record);
				backloggedRecords->set(backlogId);
				}
			
			removeRecord(record);
			}
		}
}

void Transaction::validateRecords(void)
{
	RecordVersion *record;
	Sync syncRec(&syncRecords,"Transaction::validateRecords");
	syncRec.lock(Shared);
	for (record = firstRecord; record && record->nextInTrans; record = record->nextInTrans)
		;
	
	ASSERT(lastRecord == record);
	
	for (record = lastRecord; record && record->prevInTrans; record = record->prevInTrans)
		;
	
	ASSERT(firstRecord == record);	
}
