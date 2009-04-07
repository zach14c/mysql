/* Copyright (C) 2007 MySQL AB, 2008 Sun Microsystems, Inc.

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
#include "Engine.h"
#include "RecordScavenge.h"
#include "Database.h"
#include "Record.h"
#include "RecordVersion.h"
#include "Log.h"
#include "MemMgr.h"
#include "MemControl.h"
#include "Sync.h"
#include "Transaction.h"
#include "TransactionManager.h"

RecordScavenge::RecordScavenge(Database *db, uint64 generation, bool forceScavenge)
								: database(db), baseGeneration(generation), forced(forceScavenge)
{
	cycle = ++database->scavengeCycle;

	memset(ageGroups, 0, sizeof(ageGroups));
	veryOldRecords = 0;
	veryOldRecordSpace = 0;

	startingActiveMemory = database->recordMemoryControl->getCurrentMemory(MemMgrRecordData);
	prunedActiveMemory = 0;
	retiredActiveMemory = 0;

	scavengeStart = database->deltaTime;
	pruneStop = 0;
	retireStop = 0;
	scavengeGeneration = 0;

	// Results of Scavenging
	recordsPruned = 0;
	spacePruned = 0;
	recordsRetired = 0;
	spaceRetired = 0;
	recordsRemaining = 0;
	spaceRemaining = 0;

	// Results of the inventory
	totalRecords = 0;
	totalRecordSpace = 0;
	pruneableRecords = 0;
	pruneableSpace = 0;
	retireableRecords = 0;
	retireableSpace = 0;
	unScavengeableRecords = 0;
	unScavengeableSpace = 0;
	maxChainLength = 0;

	Sync syncActive(&database->transactionManager->activeTransactions.syncObject, "RecordScavenge::RecordScavenge");
	syncActive.lock(Shared);

	oldestActiveTransaction = database->transactionManager->findOldestInActiveList();
}

RecordScavenge::~RecordScavenge(void)
{
}

bool RecordScavenge::canBeRetired(Record* record)
{
	// Check if this record can be retired

	if (record->generation <= scavengeGeneration)
		{
		// Record objects that are old enough can always be retired

		if (!record->isVersion())
			return true;

		RecordVersion * recVer = (RecordVersion *) record;
		TransactionState * transState = recVer->transactionState;

		// This record version may be retired if it is
		// currently not pointed to by a transaction.

		if (   recVer->useCount == 1
			&& !recVer->getPriorVersion()
			&& transState->committedBefore(oldestActiveTransaction)
			&& (!transState->hasTransactionReference))
			return true;
		}

	return false;
}

// Take an inventory of every record in this record chain.
// If there are any old invisible records at the end of 
// the chain, return a pointer to the oldest visible record.
// It is assumed that the record sent is the 'base' record,
// which means that the RecordLeaf has a pointer to this.
// It is what you get from a Table::fetch()
// Only base records with no priorRecords attached can be 'retired'.
// 'Pruning' involved releasing the priorRecords at the end 
// of the chain that are no longer visible to any active transaction.

Record* RecordScavenge::inventoryRecord(Record* record)
{
	uint64 chainLength = 0;
	Record *oldestVisibleRec = NULL;

	//Sync syncPrior(record->getSyncPrior(), "RecordScavenge::inventoryRecord");
	//syncPrior.lock(Shared);

	for (Record *rec = record; rec; rec = rec->getPriorVersion())
		{
		if (++chainLength > maxChainLength)
			maxChainLength = chainLength;
			
		int scavengeType = CANNOT_SCAVENGE;  // Initial value
		++totalRecords;
		int size = rec->getMemUsage();
		totalRecordSpace += size;

		// Check if this record can be scavenged somehow

		if (!rec->isVersion())
			{
			// This Record object was read from a page on disk

			if (rec == record)
				scavengeType = CAN_BE_RETIRED;  // This is a base Record object 
			else
				{
				// There must be some newer RecordVersions.

				if (oldestVisibleRec)
					scavengeType = CAN_BE_PRUNED;   // This is a Record object at the end of a chain.
				else
					oldestVisibleRec = rec;
				}
			}

		else      // This is a RecordVersion object
			{
			RecordVersion * recVer = (RecordVersion *) rec;

			// We assume here that Transaction::commitRecords is only called
			// when there are no dependent transactions.  It means that if the 
			// transaction pointer is null, and we do not know the commitId,
			// Then we can be assured that the recVer was committed before
			// any active transaction, including oldestActiveTransaction.

			bool committedBeforeAnyActiveTrans = recVer->committedBefore(oldestActiveTransaction);
			
			// This recVer 'may' be retired if it is the base record AND
			// it is currently not needed by any active transaction.

			if (recVer == record && committedBeforeAnyActiveTrans)
				scavengeType = CAN_BE_RETIRED;
			
			// Look for the oldest visible record version in this chain.

			if (oldestVisibleRec)
				{
				// Younger transactions may commit before older transactions.
				// If this older record is visible, then forget oldestVisibleRec

				if (!committedBeforeAnyActiveTrans)
					oldestVisibleRec = NULL;

				else
					{
					// Do not prune records that have other pointers to them.
					// These pointers may be temporary. By setting oldestVisibleRec 
					// to NULL, the oldestVisibleRec will move further down the 
					// chain this cycle.

					if (recVer->useCount != 1)
						oldestVisibleRec = NULL;   // Reset this pointer.
					else
						scavengeType = CAN_BE_PRUNED;
					}
				}
			else if (committedBeforeAnyActiveTrans)
				{
				if (rec->state != recLock)
					oldestVisibleRec = rec;
				}
			}

		// Add up the scavengeable space.

		switch (scavengeType)
			{
			case CAN_BE_PRUNED:
				pruneableRecords++;
				pruneableSpace += size;
				break;

			case CAN_BE_RETIRED:
				retireableRecords++;
				retireableSpace += size;
				break;

			default:
				unScavengeableRecords++;
				unScavengeableSpace += size;
			}

		// Add up all retireable records in a array of relative ages 
		// from our baseGeneration. Only base records can be retired.

		if (rec == record)
			{
			int64 age = (int64) baseGeneration - (int64) rec->generation;

			// While this inventory is happening, newer records could be
			// created that are a later generation than our baseGeneration.
			// So check for age < 0.

			if (age < 0)
				ageGroups[0] += size;
			else if (age < 1)
				ageGroups[0] += size;
			else if (age < (int64) AGE_GROUPS)
				ageGroups[age] += size;
			else	// age >= AGE_GROUPS
				{
				veryOldRecords++;
				veryOldRecordSpace += size;
				}
			}
		}

	return oldestVisibleRec;
}

uint64 RecordScavenge::computeThreshold(uint64 spaceToRetire)
{
	uint64 totalSpace = veryOldRecordSpace;
	scavengeGeneration = baseGeneration;
	
	// Don't mess around if memory is critical
	
	if (database->lowMemory)
		return scavengeGeneration;

	// The baseGeneration is the currentGeneration when the scavenge started
	// It is in ageGroups[0].  Next oldest in ageGroups[1], etc.
	// Find the youngest generation to start scavenging.
	// Scavenge that scavengeGeneration and older.

	for (int n = AGE_GROUPS - 1; n && !scavengeGeneration; n--)
		{
		totalSpace += ageGroups[n];

		if (totalSpace >= spaceToRetire)
			scavengeGeneration = baseGeneration - n;
		}

	return scavengeGeneration;
}

void RecordScavenge::print(void)
{
	Log::log(LogScavenge, "=== Scavenge Cycle " I64FORMAT " - %s - %d seconds\n",
		cycle, (const char*) database->name, retireStop - scavengeStart);

	if (!recordsPruned && !recordsRetired)
		return;

	uint64 max;

	// Find the maximum age group represented

	for (max = AGE_GROUPS - 1; max > 0; --max)
		if (ageGroups[max])
			break;

	Log::log (LogScavenge,"Cycle=" I64FORMAT 
		"  Base Generation=" I64FORMAT 
		"  Scavenge Generation=" I64FORMAT
		"  Forced=%d"
		"  Low Memory=%d\n", 
		cycle, baseGeneration, scavengeGeneration, (int)forced, (int)database->lowMemory);
	Log::log (LogScavenge,"Cycle=" I64FORMAT 
		"  Oldest Active Transaction=%d\n", 
		cycle, oldestActiveTransaction);
	Log::log (LogScavenge,"Cycle=" I64FORMAT 
		"  Threshold=" I64FORMAT 
		"  Floor=" I64FORMAT 
		"  Now=" I64FORMAT "\n", 
		cycle, database->recordScavengeThreshold, 
		database->recordScavengeFloor,
		retiredActiveMemory );
	for (uint64 n = 0; n <= max; ++n)
		if (ageGroups [n])
			Log::log (LogScavenge,"Cycle=" I64FORMAT 
				"  Age=" I64FORMAT "  Size=" I64FORMAT "\n", 
				cycle, baseGeneration - n, ageGroups[n]);
	Log::log (LogScavenge,"Cycle=" I64FORMAT 
		"  Very Old Records=" I64FORMAT " Size=" I64FORMAT "\n", 
		cycle, veryOldRecords, veryOldRecordSpace);

	// Results of the inventory

	Log::log (LogScavenge,"Cycle=" I64FORMAT 
		"  Inventory; Total records=" I64FORMAT " containing " I64FORMAT " bytes\n", 
		cycle, totalRecords, totalRecordSpace);
	Log::log (LogScavenge,"Cycle=" I64FORMAT 
		"  Inventory; Pruneable records=" I64FORMAT " containing " I64FORMAT " bytes\n", 
		cycle, pruneableRecords, pruneableSpace);
	Log::log (LogScavenge,"Cycle=" I64FORMAT 
		"  Inventory; Retireable records=" I64FORMAT " containing " I64FORMAT " bytes\n", 
		cycle, retireableRecords, retireableSpace);
	Log::log (LogScavenge,"Cycle=" I64FORMAT 
		"  Inventory; unScavengeable records=" I64FORMAT " containing " I64FORMAT " bytes\n", 
		cycle, unScavengeableRecords, unScavengeableSpace);
	Log::log (LogScavenge,"Cycle=" I64FORMAT 
		"  Inventory; max prior reord chain length=" I64FORMAT "\n", 
		cycle, maxChainLength);

	// Results of the Scavenge Cycle;

	Log::log(LogScavenge, "Cycle=" I64FORMAT 
		"  Results; Pruned " I64FORMAT " records, " I64FORMAT 
		" bytes in %d seconds\n", 
		cycle, recordsPruned, spacePruned, pruneStop - scavengeStart);
	Log::log(LogScavenge, "Cycle=" I64FORMAT 
		"  Results; Retired " I64FORMAT " records, " I64FORMAT 
		" bytes in %d seconds\n", 
		cycle, recordsRetired, spaceRetired, retireStop - pruneStop);

	if (!recordsRetired)
		{
		recordsRemaining = totalRecords - recordsPruned - recordsRetired;
		spaceRemaining = totalRecordSpace - spacePruned - spaceRetired;
		}

	Log::log(LogScavenge, "Cycle=" I64FORMAT 
		"  Results; Remaining " I64FORMAT 
		" Records, " I64FORMAT " remaining bytes\n", 
		cycle, recordsRemaining, spaceRemaining);
	Log::log (LogScavenge,"Cycle=" I64FORMAT 
		"  Results; Active memory at Scavenge Start=" I64FORMAT "\n", 
		cycle, startingActiveMemory);
	Log::log (LogScavenge,"Cycle=" I64FORMAT 
		"  Results; Active memory after Pruning Records=" I64FORMAT "\n", 
		cycle, prunedActiveMemory);
	Log::log (LogScavenge,"Cycle=" I64FORMAT 
		"  Results; Active memory after Retiring Records=" I64FORMAT "\n", 
		cycle, retiredActiveMemory );
}
