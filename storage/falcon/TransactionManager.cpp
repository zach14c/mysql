/* Copyright (C) 2006 MySQL AB, 2008 Sun Microsystems, Inc.

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


#include <memory.h>
#include <stdio.h>
#include <limits.h>
#include "Engine.h"
#include "TransactionManager.h"
#include "Transaction.h"
#include "Sync.h"
#include "Interlock.h"
#include "SQLError.h"
#include "Database.h"
#include "Connection.h"
#include "InfoTable.h"
#include "Log.h"
#include "LogLock.h"
#include "Synchronize.h"
#include "Thread.h"

static const int EXTRA_TRANSACTIONS = 10;

static TransId OLD = 0;  // Temporary for debugging

#ifdef _DEBUG
#undef THIS_FILE
static const char THIS_FILE[]=__FILE__;
#endif

volatile int Talloc = 0;  // Temp. will be removed. Used for tracing
volatile int Tdelete = 0; // new and delete of trans objects.

//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////


TransactionManager::TransactionManager(Database *db)
{
	database = db;
	transactionSequence = 1;
	committed = 0;
	rolledBack = 0;
	priorCommitted = 0;
	priorRolledBack = 0;
	rolledBackTransaction = new Transaction(database->systemConnection, 0);
	rolledBackTransaction->state = RolledBack;
	rolledBackTransaction->inList = false;
	syncObject.setName("TransactionManager::syncObject");
	activeTransactions.syncObject.setName("TransactionManager::activeTransactions");
	committedTransactions.syncObject.setName("TransactionManager::committedTransactions");
}

TransactionManager::~TransactionManager(void)
{
	rolledBackTransaction->release();
	
	for (Transaction *transaction; (transaction = activeTransactions.first);)
		{
		transaction->inList = false;
		transaction->state = Committed;
		activeTransactions.first = transaction->next;
		transaction->release();
		}
}

TransId TransactionManager::findOldestActive()
{
	Sync syncCommitted(&committedTransactions.syncObject, "TransactionManager::findOldestActive(1)");
	syncCommitted.lock(Shared);
	TransId oldestCommitted = transactionSequence;
	
	for (Transaction *trans = committedTransactions.first; trans; trans = trans->next)
		oldestCommitted = MIN(trans->transactionId, oldestCommitted);

	syncCommitted.unlock();

	Transaction *oldest = findOldest();

	if (oldest)
		{
		//Log::debug("Oldest transaction %d, oldest ancestor %d, oldest committed %d\n",  oldest->transactionId, oldest->oldestActive, oldestCommitted);

		return MIN(oldest->oldestActive, oldestCommitted);
		}
	
	//Log::debug("No active, current %d, oldest committed %d\n", transactionSequence, oldestActive);
	
	return oldestCommitted;
}

Transaction* TransactionManager::findOldest(void)
{
	Sync sync (&activeTransactions.syncObject, "TransactionManager::findOldest");
	sync.lock (Shared);
	Transaction *oldest = NULL;
	
	for (Transaction *transaction = activeTransactions.first; transaction; transaction = transaction->next)
		if (transaction->isActive() && (!oldest || transaction->transactionId < oldest->transactionId))
			oldest = transaction;
	
	return oldest;
}


TransId TransactionManager::findOldestInActiveList() const
{
	// Find the transaction id of the oldest active transaction in the 
	// active transaction list. If the list is empty, the
	// latest allocated transaction id will be returned.
	// This method assumes that the caller have set at least a shared lock
	// on the active list.

	// Note: Here we operate on a transaction list where we allow 
	// non-locking transaction allocations and de-allocations from so be 
	// careful when updating this method.

	// NOTE: This needs to be updated when we allow transaction id to wrap

	TransId oldest = transactionSequence;
	
	for (Transaction *transaction = activeTransactions.first; transaction; transaction = transaction->next)
		{
		TransId transId = transaction->transactionId;
		if (transaction->isActive() && (transId != 0 && transId < oldest))
			oldest = transId;
		}
	
	return oldest;
}


Transaction* TransactionManager::startTransaction(Connection* connection)
{
	// Go through the active transaction list to check if there are any
	// transaction objects in state "Available" that can be re-used.
	// Note that this is done using a shared lock on the active transaction
	// list.

	Sync sync (&activeTransactions.syncObject, "TransactionManager::startTransaction");
	sync.lock(Shared);
	Transaction *transaction;

	for(transaction = activeTransactions.first; transaction; transaction = transaction->next)
		if (transaction->state == Available)
			if (COMPARE_EXCHANGE(&transaction->state, Available, Initializing))
				{
				transaction->initialize(connection, INTERLOCKED_INCREMENT(transactionSequence));
				return transaction;
				}

	sync.unlock();

	// We did not find an available transaction object to re-use, 
	// so we allocate a new and add to the active list

	sync.lock(Exclusive);

	transaction = new Transaction(connection, INTERLOCKED_INCREMENT(transactionSequence));
	activeTransactions.append(transaction);

	// Since we have acquired the exclusive lock on the active transaction
	// list we allocate some extra transaction objects for future use

	for (int n = 0; n < EXTRA_TRANSACTIONS; ++n)
		{
		Transaction *trans = new Transaction(connection, 0);
		activeTransactions.append(trans);
		}

	return transaction;
}

void TransactionManager::dropTable(Table* table, Transaction* transaction)
{
	Sync committedTrans (&committedTransactions.syncObject, "TransactionManager::dropTable");
	committedTrans.lock (Shared);
	
	for (Transaction *trans = committedTransactions.first; trans; trans = trans->next)
		trans->dropTable(table);
	
	committedTrans.unlock();
}

void TransactionManager::truncateTable(Table* table, Transaction* transaction)
{
	Sync committedTrans (&committedTransactions.syncObject, "TransactionManager::truncateTable");
	committedTrans.lock (Shared);
	
	for (Transaction *trans = committedTransactions.first; trans; trans = trans->next)
		trans->truncateTable(table);
	
	committedTrans.unlock();
}

bool TransactionManager::hasUncommittedRecords(Table* table, Transaction* transaction)
{
	Sync syncTrans (&activeTransactions.syncObject, "TransactionManager::hasUncommittedRecords");
	syncTrans.lock (Shared);
	
	for (Transaction *trans = activeTransactions.first; trans; trans = trans->next)
		if (trans != transaction && trans->isActive() && trans->hasRecords(table))
			return true;

	return false;
}

// Wait until all committed records for a table are purged by gophers
// (their transaction become write complete)
void TransactionManager::waitForWriteComplete(Table* table)
{
	for(;;)
		{
		bool again = false;
		Sync committedTrans (&committedTransactions.syncObject,
			"TransactionManager::waitForWriteComplete");
		committedTrans.lock (Shared);

		for (Transaction *trans = committedTransactions.first; trans; 
			 trans = trans->next)
			{
				if (trans->hasRecords(table)&& trans->writePending)
				{
				again = true;
				break;
				}
			}

		if(!again)
			return;

		committedTrans.unlock();
		Thread::getThread("TransactionManager::waitForWriteComplete")->sleep(10);
		}
}
void TransactionManager::commitByXid(int xidLength, const UCHAR* xid)
{
	Sync sync (&activeTransactions.syncObject, "TransactionManager::commitByXid");
	sync.lock (Shared);
	
	for (bool again = true; again;)
		{
		again = false;
		
		for (Transaction *transaction = activeTransactions.first; transaction; transaction = transaction->next)
			if (transaction->state == Limbo && transaction->isXidEqual(xidLength, xid))
				{
				sync.unlock();
				transaction->commit();
				sync.lock(Shared);
				again = true;
				break;
				}
		}
}

void TransactionManager::rollbackByXid(int xidLength, const UCHAR* xid)
{
	Sync sync (&activeTransactions.syncObject, "TransactionManager::rollbackByXid");
	sync.lock (Shared);
	
	for (bool again = true; again;)
		{
		again = false;
		
		for (Transaction *transaction = activeTransactions.first; transaction; transaction = transaction->next)
			if (transaction->state == Limbo && transaction->isXidEqual(xidLength, xid))
				{
				sync.unlock();
				transaction->rollback();
				sync.lock(Shared);
				again = true;
				break;
				}
		}
}

void TransactionManager::print(void)
{
	Sync syncActive (&activeTransactions.syncObject, "TransactionManager::print(1)");
	syncActive.lock (Exclusive);

	Sync syncCommitted (&committedTransactions.syncObject, "TransactionManager::print(2)");
	syncCommitted.lock (Exclusive);

	Transaction *transaction;
	Log::debug("Active Transaction:\n");
	
	for (transaction = activeTransactions.first; transaction; transaction = transaction->next)
		transaction->print();

	syncActive.unlock();

	Log::debug("Committed Transaction:\n");
	
	for (transaction = committedTransactions.first; transaction; transaction = transaction->next)
		transaction->print();
		
}

void TransactionManager::getTransactionInfo(InfoTable* infoTable)
{
	Sync syncActive (&activeTransactions.syncObject, "TransactionManager::getTransactionInfo(2)");
	syncActive.lock (Exclusive);

	Sync syncCommitted (&committedTransactions.syncObject, "TransactionManager::getTransactionInfo(1)");
	syncCommitted.lock (Exclusive);

	Transaction *transaction;
	
	for (transaction = activeTransactions.first; transaction; transaction = transaction->next)
		transaction->getInfo(infoTable);

	syncActive.unlock();

	for (transaction = committedTransactions.first; transaction; transaction = transaction->next)
		transaction->getInfo(infoTable);
}


void TransactionManager::purgeTransactions()
{
	// This method is called by the scavenger to clean up old committed
	// transactions. 

	// To purge the committed transaction list requires at least
	// a shared lock on the active transaction list and an exclusive
	// lock on the committed transaction list

	Sync syncActive(&activeTransactions.syncObject, "TransactionManager::purgeTransaction");
	syncActive.lock(Shared);
	
	Sync syncCommitted(&committedTransactions.syncObject, "Transaction::purgeTransactions");
	syncCommitted.lock(Exclusive);

	fprintf(stderr, "TM::purgeTransactions: BEFORE: active=%d committed=%d alloc=%d delete=%d diff=%d\n", activeTransactions.count, committedTransactions.count, Talloc, Tdelete, (Talloc-Tdelete));

	purgeTransactionsWithLocks();

	fprintf(stderr, "TM::purgeTransactions: AFTER : active=%d committed=%d alloc=%d delete=%d diff=%d\n", activeTransactions.count, committedTransactions.count, Talloc, Tdelete, (Talloc-Tdelete));
}


void TransactionManager::purgeTransactionsWithLocks()
{
	// Removes old committed transaction from the committed transaction list
	// that no longer is visible by any currently active transactions.
	// Note that this method relies on that the caller have at least a
	// shared lock on the active transaction list and an exclusive lock on
	// the committed transaction list

	// Find the transaction id of the oldest active transaction

	TransId oldestActive = findOldestInActiveList();

	ASSERT(oldestActive >= OLD);   // Temporary for debugging
	OLD = oldestActive;            // Please ignore

	// Check for any fully mature transactions to ditch
  
    Transaction* transaction = committedTransactions.first;

    while ((transaction != NULL) &&
		   (transaction->state == Committed) &&
		   (transaction->commitId < oldestActive) &&
		   !transaction->writePending)
		{
		transaction->commitRecords();

		if (COMPARE_EXCHANGE(&transaction->inList, (INTERLOCK_TYPE) true, (INTERLOCK_TYPE) false))
			{
			committedTransactions.remove(transaction);
			transaction->release();
			}
		else
			{
			// If the compare and exchange operation failed we re-try this transaction on the next call

			break;
			}

		transaction = committedTransactions.first;
		}
}


void TransactionManager::getSummaryInfo(InfoTable* infoTable)
{
	Sync syncActive (&activeTransactions.syncObject, "TransactionManager::getSummaryInfo(2)");
	syncActive.lock (Exclusive);

	Sync syncCommitted (&committedTransactions.syncObject, "TransactionManager::getSummaryInfo(1)");
	syncCommitted.lock (Exclusive);

	int numberCommitted = committed;
	int numberRolledBack = rolledBack;
	int numberActive = 0;
	int numberPendingCommit = 0;
	int numberPendingCompletion = 0;

	Transaction *transaction;
	
	for (transaction = activeTransactions.first; transaction; transaction = transaction->next)
		{
		if (transaction->state == Active)
			++numberActive;
			
		if (transaction->state == Committed)
			++numberPendingCommit;
		}
	syncActive.unlock();

	for (transaction = committedTransactions.first; transaction; transaction = transaction->next)
		if (transaction->writePending)
			++numberPendingCompletion;
	
	syncCommitted.unlock();

	int n = 0;
	infoTable->putInt(n++, numberCommitted);
	infoTable->putInt(n++, numberRolledBack);
	infoTable->putInt(n++, numberActive);
	infoTable->putInt(n++, numberPendingCommit);
	infoTable->putInt(n++, numberPendingCompletion);
	infoTable->putRecord();
}

void TransactionManager::reportStatistics(void)
{
	Sync sync (&activeTransactions.syncObject, "Database::reportStatistics");
	sync.lock (Shared);
	Transaction *transaction;
	int active = 0;
	int available = 0;
	time_t maxTime = 0;
	
	for (transaction = activeTransactions.first; transaction; transaction = transaction->next)
		if (transaction->state == Active)
			{
			++active;
			time_t ageTime = database->deltaTime - transaction->startTime;
			maxTime = MAX(ageTime, maxTime);
			}
		else if (transaction->state == Available)
			{
			++available;
			}
			
	int pendingCleanup = committedTransactions.count;
	int numberCommitted = committed - priorCommitted;
	int numberRolledBack = rolledBack - priorRolledBack;
	priorCommitted = committed;
	priorRolledBack = rolledBack;
	
	if ((active || numberCommitted || numberRolledBack) && Log::isActive(LogInfo))
		Log::log (LogInfo, "%d: Transactions: %d committed, %d rolled back, %d active, %d available, %d post-commit, oldest %d seconds\n",
				  database->deltaTime, numberCommitted, numberRolledBack, active, available, pendingCleanup, maxTime);
}

void TransactionManager::removeCommittedTransaction(Transaction* transaction)
{
	Sync syncCommitted(&committedTransactions.syncObject, "TransactionManager::removeCommittedTransaction");
	syncCommitted.lock(Exclusive);
	committedTransactions.remove(transaction);
	syncCommitted.unlock();
	transaction->release();
}


Transaction* TransactionManager::findTransaction(TransId transactionId)
{
	Sync syncActive(&activeTransactions.syncObject, "TransactionManager::findTransaction(1)");
	syncActive.lock(Shared);
	Transaction *transaction;

	for (transaction = activeTransactions.first; transaction; transaction = transaction->next)
		if (transaction->transactionId == transactionId)
			return transaction;
	
	syncActive.unlock();

	Sync syncCommitted(&committedTransactions.syncObject, "TransactionManager::findTransaction(2)");
	syncCommitted.lock(Shared);

	for (transaction = committedTransactions.first; transaction; transaction = transaction->next)
		if (transaction->transactionId == transactionId)
			return transaction;
	
	return NULL;
}


void TransactionManager::removeTransaction(Transaction* transaction)
{
	if (transaction->state == Committed)
		{
		Sync sync(&committedTransactions.syncObject, "TransactionManager::removeTransaction(1)");
		sync.lock(Exclusive);
		
		for (Transaction *trans = committedTransactions.first; trans; trans = trans->next)
			if (trans == transaction)
				{
				committedTransactions.remove(transaction);
				break;
				}
		}
	else
		{
		Sync sync(&activeTransactions.syncObject, "TransactionManager::removeTransaction(2)");
		sync.lock(Exclusive);
		
		for (Transaction *trans = activeTransactions.first; trans; trans = trans->next)
			if (trans == transaction)
				{
				activeTransactions.remove(transaction);
				break;
				}
		}
}

void TransactionManager::printBlockage(void)
{
	LogLock logLock;
	Sync sync (&activeTransactions.syncObject, "TransactionManager::printBlockage");
	sync.lock (Shared);

	for (Transaction *trans = activeTransactions.first; trans; trans = trans->next)
		if (trans->state == Active && !trans->waitingFor)
			trans->printBlocking(0);

	Synchronize::freezeSystem();
}

void TransactionManager::printBlocking(Transaction* transaction, int level)
{
	for (Transaction *trans = activeTransactions.first; trans; trans = trans->next)
		if (trans->state == Active && trans->waitingFor == transaction)
			trans->printBlocking(level);
}
