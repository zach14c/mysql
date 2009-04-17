/* Copyright (C) 2006-2008 MySQL AB, 2008-2009 Sun Microsystems, Inc.

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
#include "TransactionState.h"
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
#include "SerialLogTransaction.h"
#include "InfoTable.h"
#include "Thread.h"
#include "Format.h"
#include "LogLock.h"
#include "SRLSavepointRollback.h"
#include "Bitmap.h"
#include "BackLog.h"
#include "Interlock.h"
#include "Error.h"
#include "CycleLock.h"

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
	savePoints = NULL;
	freeSavePoints = NULL;
	useCount = 1;
	syncObject.setName("Transaction::syncObject");
	syncDeferredIndexes.setName("Transaction::syncDeferredIndexes");
	syncRecords.setName("Transaction::syncRecords");
	syncSavepoints.setName("Transaction::syncSavepoints");
	firstRecord = NULL;
	lastRecord = NULL;
	transactionState = new TransactionState();
	transactionState->hasTransactionReference = true;
	initialize(cnct, seq);
}

void Transaction::initialize(Connection* cnct, TransId seq)
{
	Sync sync(&syncObject, "Transaction::initialize(1)");
	sync.lock(Exclusive);
	ASSERT(savePoints == NULL);
	ASSERT(freeSavePoints == NULL);
	ASSERT(firstRecord == NULL);
	connection = cnct;
	isolationLevel = connection->isolationLevel;
	mySqlThreadId = connection->mySqlThreadId;
	database = connection->database;
	transactionManager = database->transactionManager;
	systemTransaction = database->systemConnection == connection;
	transactionId = seq;
	transactionState->transactionId = seq;
	transactionState->commitId = 0;
	chillPoint = &firstRecord;
	commitTriggers = false;
	hasUpdates = false;
	hasLocks = false;
	writePending = true;
	//pendingPageWrites = false;
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
	blockedBy = 0;
	deletedRecords = 0;
	inList = true;
	thread = NULL;
	
	if (seq == 0)
		{
		transactionState->state = Available;
		systemTransaction = false;
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
	transactionState->syncIsActive.lock(NULL, Exclusive);
	transactionState->state = Active;
}

Transaction::~Transaction()
{
	if (transactionState->state == Active)
		{
		Log::debug("Deleting apparently active transaction %d\n", transactionId);
		ASSERT(false);
		
		if (transactionState->syncIsActive.ourExclusiveLock())
			transactionState->syncIsActive.unlock();
		}

	if (inList)
		transactionManager->removeTransaction(this);

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

	if (transactionState)
		{
		transactionState->hasTransactionReference = false;
		transactionState->release();
		}
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

	addRef();
	Log::log(LogXARecovery, "%d: Commit %sTransaction %d\n", 
		database->deltaTime, (systemTransaction ? "System " : ""),  transactionId);

	if (transactionState->state == Active)
		{
		Sync sync(&syncDeferredIndexes, "Transaction::commit(1)");
		sync.lock(Shared);
		
		for (DeferredIndex *deferredIndex= deferredIndexes; deferredIndex;  
			 deferredIndex = deferredIndex->nextInTransaction)
			if (deferredIndex->index)
				database->dbb->logIndexUpdates(deferredIndex);
		
		sync.unlock();
		database->dbb->logUpdatedRecords(this, firstRecord);

		if (transactionState->pendingPageWrites)
			database->pageWriter->waitForWrites(this);
		}
			
	++transactionManager->committed;

	if (hasLocks)
		releaseRecordLocks();

	database->serialLog->preCommit(this);

	Sync syncRec(&syncRecords,"Transaction::commit(1.5)");
	syncRec.lock(Shared);
	
	for (RecordVersion *record = firstRecord; record; record = record->nextInTrans)
		{
		RECORD_HISTORY(record);
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
	database->flushInversion(this);

	// Write the commit message to the serial log for durability.
	// If a crash happens after this, the recover will commit.

	database->serialLog->logControl->commit.append(this);

	// Transfer transaction from active list to committed list, set committed state

	Sync syncActiveTransactions(&transactionManager->activeTransactions.syncObject, "Transaction::commit(2)");
	Sync syncCommitted(&transactionManager->committedTransactions.syncObject, "Transaction::commit(3)");

	syncActiveTransactions.lock(Exclusive);
	syncCommitted.lock(Exclusive);

	// Set the commit transition id for this transaction

	transactionState->commitId = INTERLOCKED_INCREMENT(transactionManager->transactionSequence);

	transactionManager->activeTransactions.remove(this);
	transactionManager->committedTransactions.append(this);
	transactionState->state = Committed;

	// This is one of the few points where we have an exclusive lock on both the
	// active and committed transaction list. Although this has nothing to do
	// with the commit of this transaction we use the opportunity to clean up
	// old transaction objects

	// Temporarily disable this call and let the scavenger delete all
	// old transactions object (see bug 41357)

	// transactionManager->purgeTransactionsWithLocks();

	syncCommitted.unlock();
	syncActiveTransactions.unlock();
	
	transactionState->syncIsActive.unlock(); // signal waiting transactions
	
	// signal a gopher to start processing this transaction

	SerialLogTransaction *srlTransaction = database->serialLog->getTransaction(transactionId);
	srlTransaction->setState(sltCommitted);
	database->serialLog->wakeup();

	delete [] xid;
	xid = NULL;
	xidLength = 0;
	
	// If there's no reason to stick around, just go away

	connection = NULL;
	
	// Add ourselves to the list of lingering committed transactions
	
	release();
}


void Transaction::commitNoUpdates(void)
{
	addRef();
	ASSERT(!deferredIndexes);
	Log::log(LogXARecovery, "%d: CommitNoUpdates transaction %d\n", database->deltaTime, transactionId);
	++transactionManager->committed;
	
	if (deferredIndexes)
		releaseDeferredIndexes();

	if (hasLocks)
		releaseRecordLocks();

	Sync syncActiveTransactions(&transactionManager->activeTransactions.syncObject, "Transaction::commitNoUpdates(2)");
	syncActiveTransactions.lock(Shared);

	if (xid)
		{
		delete [] xid;
		xid = NULL;
		xidLength = 0;
		}
	
	Sync sync(&syncObject, "Transaction::commitNoUpdates(3)");
	sync.lock(Exclusive);

	// If there's no reason to stick around, just go away
	
	connection = NULL;
	transactionId = 0;
	transactionState->transactionId = 0;
	writePending = false;
	transactionState->state = Available;
	syncActiveTransactions.unlock();
	transactionState->syncIsActive.unlock();
	release();
}

void Transaction::rollback()
{
	RecordVersion *stack = NULL;
	RecordVersion *record;

	if (!isActive())
		throw SQLEXCEPTION (RUNTIME_ERROR, "transaction is not active");

	Log::log(LogXARecovery, "%d: Rollback transaction %d\n", database->deltaTime, transactionId);

	if (deferredIndexes)
		releaseDeferredIndexes();
		
	releaseSavepoints();
	chillPoint = &firstRecord;
	totalRecordData = 0;
	totalRecords = 0;

	// Rollback pending record versions from newest to oldest in case
	// there are multiple record versions on a prior record chain

	Sync syncRec(&syncRecords, "Transaction::rollback(records)");
	syncRec.lock(Exclusive);

	while (firstRecord)
		{
		record = firstRecord;
		RECORD_HISTORY(record);
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
			record->format->table->unlockRecord(record, 0);
		else
			record->rollback(this);
		
		//record->transaction = rollbackTransaction;
		//record->release(REC_HISTORY);
		record->queueForDelete();
		}
		
	firstRecord = NULL;
	syncRec.unlock();

	Sync syncSP(&syncSavepoints, "Transaction::rollback");
	syncSP.lock(Shared);
	
	for (SavePoint *savePoint = savePoints; savePoint; savePoint = savePoint->next)
		if (savePoint->backloggedRecords)
			database->backLog->rollbackRecords(savePoint->backloggedRecords, this);

	syncSP.unlock();

	if (backloggedRecords)
		database->backLog->rollbackRecords(backloggedRecords, this);

	ASSERT(writePending);
	writePending = false;

	if (hasUpdates)
		{
		database->serialLog->preCommit(this);
		database->serialLog->logControl->rollback.append(this);
		}

	if (xid)
		{
		delete [] xid;
		xid = NULL;
		xidLength = 0;
		}
	
	Sync syncActiveTransactions (&transactionManager->activeTransactions.syncObject, "Transaction::rollback(active)");
	syncActiveTransactions.lock (Exclusive);
	++transactionManager->rolledBack;

	inList = false;
	transactionManager->activeTransactions.remove(this);
	syncActiveTransactions.unlock();
	transactionState->state = RolledBack;
	transactionState->syncIsActive.unlock();

	// Finish the SerialLogTransaction and signal a gopher

	if (hasUpdates)
		{
		SerialLogTransaction *srlTransaction = database->serialLog->getTransaction(transactionId);
		srlTransaction->setState(sltRolledBack);
		database->serialLog->wakeup();
		}

	release();
}


void Transaction::prepare(int xidLen, const UCHAR *xidPtr)
{
	if (transactionState->state != Active)
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
	transactionState->state = Limbo;

	// Flush a prepare record to the serial log

	database->serialLog->logControl->prepare.append(transactionId, xidLength, xid);

	Sync sync(&syncDeferredIndexes, "Transaction::prepare");
	sync.lock(Shared);
	
	for (DeferredIndex *deferredIndex= deferredIndexes; deferredIndex;  
		deferredIndex = deferredIndex->nextInTransaction)
		if (deferredIndex->index)
			database->dbb->logIndexUpdates(deferredIndex);
	
	sync.unlock();
	database->dbb->logUpdatedRecords(this, firstRecord);

	if (transactionState->pendingPageWrites)
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
	
	// At the start of a chill operation, all savepoints are updated with the id of the
	// savepoint being chilled. This ensures that each savepoint in a transaction always
	// has a bitmap of savepoints that were chilled after it.

	// When a savepoint is rolled back, those newer savepoints for which records have also
	// been chilled are recorded in the serial log.

	// The idea is that if savepoint N is rolled back, then chilled records attached to
	// savepoints >= N	are ignored and not committed to the database.

	Sync syncSP(&syncSavepoints, "Transaction::rollback");
	syncSP.lock(Shared);

	for (SavePoint *savePoint = savePoints; savePoint; savePoint = savePoint->next)
		if (savePoint->id != curSavePointId)
			savePoint->setIncludedSavepoint(curSavePointId);

	syncSP.unlock();

	if (database->lowMemory && !systemTransaction)
		backlogRecords();

	Log::log(LogInfo, "%d: Record chill: transaction %ld, %ld records, %ld bytes\n", database->deltaTime, transactionId, chilledRecords-chilledBefore,
				(uint32)(totalDataBefore-totalRecordData), committedRecords);
}

int Transaction::thaw(RecordVersion * record)
{
	// Nothing to do if record is no longer chilled
	
	if (record->state != recChilled)
		return record->getDataMemUsage();
		
	// Get pointer to record data in serial log

	SerialLogControl control(database->dbb->serialLog);
	
	// Thaw the record then update the total record data bytes for this transaction
	
	ASSERT(record->getTransactionId() == transactionId);
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
	
	// If the transaction size has exceeded the chill threshold,
	// write the pending records to the serial log and free the record data.
	//
	// Never do this for system transactions.

	if (totalRecordData > database->configuration->recordChillThreshold
		&& !systemTransaction)
		{
		UCHAR saveState = record->state;
		
		// Chill all records except the current record, which may be part of an update or insert

		if (record->state != recLock && record->state != recChilled)
			record->state = recNoChill;
			
		chillRecords();

		if (record->state == recNoChill)
			record->state = saveState;
		}

	if (database->lowMemory && !systemTransaction
		&& deletedRecords > MAX_LOW_MEMORY_RECORDS)
		backlogRecords();

	// Now that Chilling and Backlogging is done for this transaction, 
	// it is safe to add the current record

	record->addRef(REC_HISTORY);

	Sync syncRec(&syncRecords,"Transaction::addRecord");
	syncRec.lock(Exclusive);

	if ( (record->prevInTrans = lastRecord) )
		lastRecord->nextInTrans = record;
	else
		firstRecord = record;

	record->nextInTrans = NULL;
	lastRecord = record;
	syncRec.unlock();
	
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
	//record->transaction = NULL;

	Sync syncSP(&syncSavepoints, "Transaction::rollback");
	syncSP.lock(Shared);

	for (SavePoint *savePoint = savePoints; savePoint; savePoint = savePoint->next)
		if (savePoint->records == &record->nextInTrans)
			savePoint->records = ptr;

	syncSP.unlock();

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
	
	record->release(REC_HISTORY);
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

	return visible(transaction->transactionState, forWhat);
}


/***
@brief		Determine if changes by another transaction are visible to this.
@details	This function is called for Consistent-Read transactions to determine
			if the sent trans was committed before this transaction started.  If not,
			it is invisible to this transaction.
***/

bool Transaction::visible(const TransactionState* transState, int forWhat) const
{
	ASSERT(transState != NULL);

	// If we're the transaction in question, consider us committed

	if (transactionState == transState)
		return true;

	// If we're the system transaction, just use the state of the other transaction

	if (database->systemConnection->transaction == this)
		return transState->state == Committed;

	// If the other transaction is not yet committed, the trans is not visible.

	if (transState->state != Committed)
		return false;

	// The other transaction is committed.  
	// If this is READ_COMMITTED, it is visible.

	if (   IS_READ_COMMITTED(isolationLevel)
		|| (   IS_WRITE_COMMITTED(isolationLevel)
		    && (forWhat == FOR_WRITING)))
		return true;

	// This is REPEATABLE_READ
	ASSERT (IS_REPEATABLE_READ(isolationLevel));

	// If the other transaction committed after we started then it is not
	// be visible to us

	if (transState->commitId > transactionId)
		return false;

	return true;
}

/***
@brief		Determine if there is a need to lock this record for update.
***/

bool Transaction::needToLock(Record* record)
{
	// Find the first visible record version

	for (Record* candidate = record;  candidate != NULL; candidate = candidate->getPriorVersion())
		{
		TransactionState* transState = candidate->getTransactionState();
		//ASSERT(transState != NULL);

		// If there is no transaction state, it is not a record version.
		
		if (!transState)
			return true;
	
		if (visible(transState, FOR_WRITING))
			{
			if (candidate->state == recDeleted)
				{
				if (transState->state == Committed)
					return false; // Committed and deleted

				return true; // Just in case this rolls back.
				}

			return true;
			}
		}

	return false;
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
				record->release(REC_HISTORY);
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
	// If this is a Record object it has no associated transaction
	// and is always visible.
	
	if (!record->isVersion())
		return CommittedVisible;

	// This RecordVersion MUST have a TransState with a reference count.
	// The caller has a reference count on record, and the record has a
	// useCount on transState.

	blockingRecord = record;
	TransactionState * transactionState = record->getTransactionState();
	ASSERT(transactionState);
	State state = getRelativeState(transactionState, flags);
	blockingRecord = NULL;

	return state;
}

/***
@brief		Get the relative state between this transaction and another.
***/

State Transaction::getRelativeState(TransactionState* transState, uint32 flags)
{
	TransId transId = transState->transactionId;
	if (transactionId == transId)
		return Us;

	// The following if test replaces the original if (!transaction)
	// This could be improved by combining it with the last if test
	// at the end of this function.

	if (transState->state == Committed && transState->commitId < transactionId)
		{
		// All calls to getRelativeState are for the purpose of writing.
		// So only ConsistentRead can get CommittedInvisible.

		if (IS_CONSISTENT_READ(isolationLevel))
			{
			// Be sure that transaction was not active when we started.
			// If the transaction is no longer connected to the record,
			// then it must be committed.  The scavenger can scavenge 
			// transactions newer than the oldest active if they are 
			// committed.

			if (transactionId < transId)
				return CommittedInvisible;
			}

		return CommittedVisible;
		}

	if (transState->isActive())
		{
		if (flags & DO_NOT_WAIT)
			return Active;

		bool isDeadlock;
		waitForTransaction(transState, &isDeadlock);

		if (isDeadlock)
			return Deadlock;

		return WasActive;			// caller will need to re-fetch
		}

	if (transState->state == Committed)
		{
		// Return CommittedVisible if the other trans has a lower TransId and 
		// it was committed when we started.
		
		if (visible (transState, FOR_WRITING))
			return CommittedVisible;

		return CommittedInvisible;
		}

	return (State) transState->state;
}

void Transaction::dropTable(Table* table)
{
	releaseDeferredIndexes(table);

	Sync syncRec(&syncRecords,"Transaction::dropTable(2)");
	syncRec.lock(Exclusive);
	
	for (RecordVersion **ptr = &firstRecord, *rec; (rec = *ptr);)
		{
		RECORD_HISTORY(rec);
		
		if (rec->format->table == table)
			removeRecord(rec);
		else
			ptr = &rec->nextInTrans;
		}
}

void Transaction::truncateTable(Table* table)
{
	releaseDeferredIndexes(table);
	Sync syncRec(&syncRecords,"Transaction::truncateTable(2)");
	syncRec.lock(Exclusive);
	
	for (RecordVersion **ptr = &firstRecord, *rec; (rec = *ptr);)
		{
		RECORD_HISTORY(rec);
		
		if (rec->format->table == table)
			removeRecord(rec);
		else
			ptr = &rec->nextInTrans;
		}
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
	ASSERT(transactionState->state == Committed);
	releaseDeferredIndexes();

//	Log::log(LogXARecovery, "%d: WriteComplete %sTransaction %d\n", 
// 	database->deltaTime, (systemTransaction ? "System " : ""),  transactionId);

	writePending = false;
}

bool Transaction::waitForTransaction(TransactionState *transState)
{
	bool deadlock;
	State state = waitForTransaction(transState, &deadlock);

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

State Transaction::waitForTransaction(TransactionState* transState, bool *deadlock)
{
	*deadlock = false;
	State state;

	// Increase the use count on the transaction state object to ensure
	// the object the waitingFor pointer refers to does not get deleted

	ASSERT(transState != NULL);

	Sync syncActiveTransactions(&transactionManager->activeTransactions.syncObject,
								"Transaction::waitForTransaction(1)");
	syncActiveTransactions.lock(Shared);

	if (transState->state == Available || transState->state == Committed)
		{
		state = (State) transState->state;

		return state;
		}

	if (!COMPARE_EXCHANGE_POINTER(&transactionState->waitingFor, NULL, transState))
		FATAL("waitingFor was not NULL");

	volatile TransactionState *trans;
	
	for (trans = transState->waitingFor; trans; trans = trans->waitingFor)
		if (trans == transactionState)
			{
			*deadlock = true;
			
			break;
			}

	// Release the lock on the active transaction list because we will
	// possibly be blocked and will re-take the lock exclusively

	syncActiveTransactions.unlock();

	if (!(*deadlock))
		{
		try
			{
			CycleLock *cycleLock = CycleLock::unlock();
			transState->waitForTransaction();

			if (cycleLock)
				cycleLock->lockCycle();
			}
		catch(...)
			{
			if (!COMPARE_EXCHANGE_POINTER(&transactionState->waitingFor, transState, NULL))
				FATAL("waitingFor was not %p", transState);

			// See comments about this locking further down

			syncActiveTransactions.lock(Exclusive);
			throw;
			}
		}

	if (!COMPARE_EXCHANGE_POINTER(&transactionState->waitingFor, transState, NULL))
		FATAL("waitingFor was not %p", transState);

	// Before returning we need to aquire an exclusive lock on 
	// syncActiveTransactions. This acts as a 'pump' to make sure that
	// no thread is still using the object that waitingFor was pointing to.
	// Traversals of the waitingFor list are done with shared locks on
	// syncActiveTransactions.  So this exclusive lock waits for those 
	// threads to finish using those pointers.

	syncActiveTransactions.lock(Exclusive);

	state = (State) transState->state;
	return state;
}

void Transaction::addRef()
{
	INTERLOCKED_INCREMENT(useCount);
}

void Transaction::release()
{
	if (INTERLOCKED_DECREMENT(useCount) == 0)
		delete this;
}

int Transaction::createSavepoint()
{
	SavePoint *savePoint;
	Sync sync(&syncSavepoints, "Transaction::createSavepoint");
	sync.lock(Exclusive);
	
	ASSERT((savePoints || freeSavePoints) ? (savePoints != freeSavePoints) : true);
	
	if ( (savePoint = freeSavePoints) )
		freeSavePoints = savePoint->next;
	else
		savePoint = new SavePoint;
	
	// The savepoint begins with the next record added to the transaction
	
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
	sync.lock(Exclusive);

	// The savePoints list goes from newest to oldest within this transaction.

	for (SavePoint **ptr = &savePoints, *savePoint; (savePoint = *ptr); ptr = &savePoint->next)
		if (savePoint->id == savePointId)
			{
			// Savepoints are linked in descending order, so the next lower id is next on the list

			int nextLowerSavePointId = (savePoint->next) ? savePoint->next->id : 0;
			*ptr = savePoint->next;

			// If we have backlogged records, merge them in to the 
			// previous savepoint or the transaction itself

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

			// This savepoint is no longer needed, so commit pending 
			// record versions to the next pending savepoint
			// Scavenge prior record versions having 
			// 1) the same transaction and 
			// 2) savepoint >= the savepoint being released

			for (RecordVersion *record = *savePoint->records; 
				 record && record->savePointId == savePointId; 
				 record = record->nextInTrans)
				{
				record->savePointId = nextLowerSavePointId;
				record->scavengeSavepoint(this, nextLowerSavePointId);
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
	SavePoint *savePoint;
	Sync sync(&syncSavepoints, "Transaction::releaseSavepoints");
	sync.lock(Exclusive);
	
	// System transactions require an exclusive lock for concurrent access

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
	SavePoint *savePoint;
	//validateRecords();
	Sync sync(&syncSavepoints, "Transaction::rollbackSavepoint");
	sync.lock(Exclusive);

	// Be sure the target savepoint is valid before rolling them back.

	for (savePoint = savePoints; savePoint; savePoint = savePoint->next)
		if (savePoint->id <= savePointId)
			break;
			
	if ((savePoint) && (savePoint->id != savePointId))
		throw SQLError(RUNTIME_ERROR, "invalid savepoint");

	// Records within this savepoint or later may have been chilled and are
	// already in the serial log, but they are now obsolete. To ensure that those
	// records are not committed to the database, append the serial log with a SRLSavepointRollback
	// record for this savepoint and for any greater savepoint that has been chilled.
	
	if (chilledRecords)
		{
		database->serialLog->logControl->savepointRollback.append(transactionId, savePointId);
		
		// SavePoint::savepoints is a bitmap of other savepoints that have been chilled
		
		if (savePoint->savepoints)
			for (int n = savePointId; (n = savePoint->savepoints->nextSet(n)) >= savePointId; ++n)
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
			SET_RECORD_ACTIVE(rec, false);
			//rec->transaction = NULL;
			//rec->release(REC_HISTORY);
			rec->queueForDelete();
			}
			
		syncRec.unlock();

		// Handle any backlogged records
		
		if (savePoint->backloggedRecords)
			database->backLog->rollbackRecords(savePoint->backloggedRecords, this);
				
		// Move skipped savepoint objects to the free list
		
		if (savePoint->id >= savePointId)
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
	Sync sync(&syncDeferredIndexes, "Transaction::add");
	sync.lock(Exclusive);

//	deferredIndex->addRef(); // temporarily disabled for Bug#39711
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
	Sync syncRec(&syncRecords,"Transaction::releaseRecordLocks");
	syncRec.lock(Exclusive);

	for (RecordVersion *record = firstRecord; record; record = record->nextInTrans)
		if (record->state == recLock)
			{
			record->format->table->unlockRecord(record, 0);

			// Don't do removeRecord(record) now.  Other threads might be 
			// pointing to it and need the transaction pointer to determine 
			// its relative state
			}

	syncRec.unlock();
}

void Transaction::print(void)
{
	Log::debug("  %p Id %d, state %d, updates %d, wrtPend %d, records %d\n",
			this, transactionId, transactionState->state, hasUpdates, writePending, 
			firstRecord != NULL);
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
	transactionManager->printBlocking(this, level);
}

void Transaction::getInfo(InfoTable* infoTable)
{
	// NOTE: The field for number of dependencies will be removed in
	// a follow-up patch.
	// Need to decide if we want to include the startEvent and endEvent
	// in this table.

	if (!(transactionState->state == Available))
		{
		int n = 0;
		infoTable->putString(n++, stateNames[transactionState->state]);
		infoTable->putInt(n++, mySqlThreadId);
		infoTable->putInt(n++, transactionId);
		infoTable->putInt(n++, hasUpdates);
		infoTable->putInt(n++, writePending);
		infoTable->putInt(n++, 0);  // Number of dependencies, will be removed
		infoTable->putInt(n++, 0); //  was oldestActive);
		infoTable->putInt(n++, firstRecord != NULL);
		infoTable->putInt(n++, (transactionState->waitingFor) ? transactionState->waitingFor->transactionId : 0);
		
		char buffer[512];
		
		if (connection)
			connection->getCurrentStatement(buffer, sizeof(buffer));
		else
			buffer[0] = 0;

		infoTable->putString(n++, buffer);
		infoTable->putRecord();
		}
}

// Called by the gopher thread to complete this transaction

void Transaction::fullyCommitted(void)
{
	ASSERT(inList);
	ASSERT(!isActive());

	if (useCount < 2)
		Log::debug("Transaction::fullyCommitted: Unusual use count=%d\n", useCount);

	writeComplete();
	releaseCommittedTransaction();
}

void Transaction::releaseCommittedTransaction(void)
{
	// NOTE: consider to just move the call to release() to where this method is called.
	// Leave it in here in case we want to check for being able to delete the transaction
	// object here.

	release();
}


void Transaction::printBlockage(void)
{
	LogLock logLock;
	Sync sync (&transactionManager->activeTransactions.syncObject, "Transaction::printBlockage");
	sync.lock (Shared);
	printBlocking(0);
}

void Transaction::releaseDeferredIndexes(void)
{
	Sync sync(&syncDeferredIndexes, "Transaction::releaseDeferredIndexes");
	sync.lock(Exclusive);
	
	for (DeferredIndex *deferredIndex; (deferredIndex = deferredIndexes);)
		{
		ASSERT(deferredIndex->transaction == this);
		deferredIndexes = deferredIndex->nextInTransaction;
		deferredIndex->detachTransaction();
		deferredIndex->release();
		deferredIndexCount--;
		}
}

void Transaction::releaseDeferredIndexes(Table* table)
{
	Sync sync(&syncDeferredIndexes, "Transaction::releaseDeferredIndexes(Table *)");
	sync.lock(Exclusive);

	for (DeferredIndex **ptr = &deferredIndexes, *deferredIndex; (deferredIndex = *ptr);)
		{
		if (deferredIndex->index && (deferredIndex->index->table == table))
			{
			*ptr = deferredIndex->nextInTransaction;
			deferredIndex->detachTransaction();
			deferredIndex->release();
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

		if (!record->hasRecord(false))
			{
			Sync syncSP(&syncSavepoints, "Transaction::backlogRecords");
			syncSP.lock(Shared);

			if (savePoints)
				for (; savePoint && record->savePointId < savePoint->id; savePoint = savePoint->next)
					;	

			if (savePoint)
				savePoint->backlogRecord(record);
			else
				{
				if (!backloggedRecords)
					backloggedRecords = new Bitmap;

				// Backlog the record and remove it from the transaction
				
				if (record->format->table->backlogRecord(record, backloggedRecords))
					removeRecord(record);
				}
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

// Return true if this transaction was committed before
// another transaction started.  If commitId is 0, then
// this trans is not yet committed.

bool Transaction::committedBefore(TransId transactionId)
{
	return (transactionState->commitId && transactionState->commitId < transactionId);
}
