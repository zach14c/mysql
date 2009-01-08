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
#include "Sync.h"
#include "Transaction.h"
#include "TransactionManager.h"

RecordScavenge::RecordScavenge(Database *db)
{
	database = db;
	cycle = ++database->scavengeCycle;

	memset(ageGroups, 0, sizeof(ageGroups));
	veryOldRecords = 0;
	veryOldRecordSpace = 0;

	startingActiveMemory = db->recordDataPool->activeMemory;
	prunedActiveMemory = 0;
	retiredActiveMemory = 0;

	scavengeStart = db->deltaTime;
	pruneStop = 0;
	retireStop = 0;

	baseGeneration = database->currentGeneration;
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

	Sync syncActive(&db->transactionManager->activeTransactions.syncObject, "RecordScavenge::RecordScavenge");
	syncActive.lock(Shared);

	oldestActiveTransaction = db->transactionManager->findOldestInActiveList();
}

RecordScavenge::~RecordScavenge(void)
{
}

bool RecordScavenge::canBeRetired(Record* record)
{
	// Check if this record can be retired

	if (record->generation <= scavengeGeneration)
		{
		// Record objects are read from pages

		if (!record->isVersion())
			return true;

		RecordVersion * recVer = (RecordVersion *) record;
		ASSERT(!recVer->superceded);  // Must be the base record

		// This record version may be retired if 
		// it is currently not pointed to by a transaction

		if (!recVer->transaction)
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
	Record *oldestVisibleRec = NULL;

	Sync syncPrior(record->getSyncPrior(), "RecordScavenge::inventoryRecord");
	syncPrior.lock(Shared);

	for (Record *rec = record; rec; rec = rec->getPriorVersion())
		{
		int scavengeType = CANNOT_SCAVENGE;  // Initial value

		++totalRecords;
		int size = rec->getMemUsage();
		totalRecordSpace += size;

		// Check if this record can be scavenged somehow

		if (rec->isVersion())
			{
			RecordVersion * recVer = (RecordVersion *) rec;

			bool committedBeforeAnyActiveTrans = false;
			if (  !recVer->transaction
				|| recVer->transaction->committedBefore(oldestActiveTransaction))
				committedBeforeAnyActiveTrans = true;

			// This record may be retired if it is the base record AND
			// it is currently not needed by any active transaction.

			if (recVer == record && committedBeforeAnyActiveTrans)
				scavengeType = CAN_BE_RETIRED;
			
			// Look for the oldest visible record version in this chain.
			// If the transaction is null then there are no dependent transactions
			// and this record version is visible to all.  If the transaction is not null,
			// then check if it ended before the current oldest active trans started.

			if (oldestVisibleRec)
				{
				if (recVer->useCount > 1)
					// TBD - A prunable record has an extra use count!  Why? by who?
					oldestVisibleRec = rec; // reset so that this recVer is not pruned.

				scavengeType = CAN_BE_PRUNED;
				}
			else if (committedBeforeAnyActiveTrans)
				{
				ASSERT(rec->state != recLock);
				oldestVisibleRec = rec;
				}
			}
		else if (oldestVisibleRec)
			scavengeType = CAN_BE_PRUNED;   // This is a Record object at the end of a chain.
		else
			scavengeType = CAN_BE_RETIRED;  // This is a base Record object 

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

		// Only base records can be retired.  Add up all retireable records 
		// in a array of relative ages from our baseGeneration.

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
			else if (age < AGE_GROUPS)
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
	scavengeGeneration = 0;

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
		"  Scavenge Generation=" I64FORMAT "\n", 
		cycle, baseGeneration, scavengeGeneration);
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
		recordsRemaining = totalRecords - recordsPruned;
		spaceRemaining = totalRecordSpace - spacePruned;
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
