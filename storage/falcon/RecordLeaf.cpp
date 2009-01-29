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

// RecordLeaf.cpp: implementation of the RecordLeaf class.
//
//////////////////////////////////////////////////////////////////////

#include <stdio.h>
#include <memory.h>
#include "Engine.h"
#include "RecordLeaf.h"
#include "RecordGroup.h"
#include "RecordVersion.h"
#include "Table.h"
#include "Sync.h"
#include "Interlock.h"
#include "Bitmap.h"
#include "RecordScavenge.h"

#ifdef _DEBUG
#undef THIS_FILE
static const char THIS_FILE[]=__FILE__;
#endif

//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////

RecordLeaf::RecordLeaf()
{
	base = 0;
	memset (records, 0, sizeof (records));
	syncObject.setName("RecordLeaf::syncObject");
}

RecordLeaf::~RecordLeaf()
{
	for (int n = 0; n < RECORD_SLOTS; ++n)
		if (records[n])
			{
#ifdef CHECK_RECORD_ACTIVITY
			for (Record *rec = records[n]; rec; rec = rec->getPriorVersion())
				rec->active = false;
#endif
				
			records[n]->release();
			}
}

Record* RecordLeaf::fetch(int32 id)
{
	if (id >= RECORD_SLOTS)
		return NULL;

	Sync sync(&syncObject, "RecordLeaf::fetch");
	sync.lock(Shared);

	Record *record = records[id];

	if (record)
		record->addRef();

	return record;
}


bool RecordLeaf::store(Record *record, Record *prior, int32 id, RecordSection **parentPtr)
{
	// If this doesn't fit, create a new level above use, then store
	// record in new record group.

	if (id >= RECORD_SLOTS)
		{
		RecordGroup* group = new RecordGroup (RECORD_SLOTS, 0, this);
		
		if (COMPARE_EXCHANGE_POINTER(parentPtr, this, group))
			 return group->store(record, prior, id, parentPtr);

		group->records[0] = NULL;
		delete group;
		
		return (*parentPtr)->store(record, prior, id, parentPtr);
		}

	// If we're adding a new version, don't bother with a lock.  Otherwise we need to lock out
	// simultaneous fetches to avoid a potential race between addRef() and release().

	if (record && record->getPriorVersion() == prior)
		{
		if (!COMPARE_EXCHANGE_POINTER(records + id, prior, record))
			return false;
		}
	else
		{
		Sync sync(&syncObject, "RecordLeaf::store");
		sync.lock(Exclusive);

		if (!COMPARE_EXCHANGE_POINTER(records + id, prior, record))
			return false;
		}

	return true;
}

// Prune old invisible record versions from the end of record chains.
// The visible versions at the front of the list are kept.
// Also, inventory each record slot on this leaf.

void RecordLeaf::pruneRecords (Table *table, int base, RecordScavenge *recordScavenge)
{
	Record **ptr, **end;

	// Get a shared lock since we are just traversing the tree.  
	// pruneRecords does not empty any slots in a record leaf.

	Sync sync(&syncObject, "RecordLeaf::pruneRecords(syncObject)");
	sync.lock(Shared);

	for (ptr = records, end = records + RECORD_SLOTS; ptr < end; ++ptr)
		{
		Record *record = *ptr;
		
		if (record)
			{
			Record* oldestVisible = recordScavenge->inventoryRecord(record);

			// Prune invisible records.

			if (oldestVisible)
				{
				ASSERT(oldestVisible->state != recLock);

				Record *prior = oldestVisible->clearPriorVersion();

				for (Record *prune = prior; prune; prune = prune->getPriorVersion())
					{
					if (prune->useCount != 1)
						{
						prior = NULL;
						break;
						}
					recordScavenge->recordsPruned++;
					recordScavenge->spacePruned += prune->getMemUsage();
					}

				if (prior)
					{
#ifdef CHECK_RECORD_ACTIVITY
					prior->active = false;
#endif
					table->garbageCollect(prior, record, NULL, false);
					prior->release();
					}
				}
			}
		}
}

void RecordLeaf::retireRecords (Table *table, int base, RecordScavenge *recordScavenge)
{
	int count = 0;
	Record **ptr, **end;

	Sync sync(&syncObject, "RecordLeaf::retireRecords(syncObject)");
	sync.lock(Shared);

	// Get a shared lock to find at least one record to scavenge

	for (ptr = records, end = records + RECORD_SLOTS; ptr < end; ++ptr)
		{
		Record *record = *ptr;

		if (record && recordScavenge->canBeRetired(record))
			break;
		}

	if (ptr >= end)
		return;

	// We can retire at least one record from this leaf;
	// Get an exclusive lock and retire as many as possible.

	sync.unlock();
	sync.lock(Exclusive);

	for (ptr = records; ptr < end; ++ptr)
		{
		Record *record = *ptr;
		
		if (record && recordScavenge->canBeRetired(record))
			{
			if (record->retire(recordScavenge))
				*ptr = NULL;
			else
				count++;
			}
		}

	// If this node is empty, store the base record number for use as an
	// identifier when the leaf node is scavenged later.

	if (!count && table->emptySections)
		table->emptySections->set(base);

	return;
}

bool RecordLeaf::retireSections(Table * table, int id)
{
	return inactive();
}

bool RecordLeaf::inactive()
{
	return (!anyActiveRecords());
}

int RecordLeaf::countActiveRecords()
{
	int count = 0;

	for (Record **ptr = records, **end = records + RECORD_SLOTS; ptr < end; ++ptr)
		if (*ptr)
			++count;

	return count;
}

bool RecordLeaf::anyActiveRecords()
{
	for (Record **ptr = records, **end = records + RECORD_SLOTS; ptr < end; ++ptr)
		if (*ptr)
			return true;

	return false;
}

int RecordLeaf::chartActiveRecords(int *chart)
{
	int count = countActiveRecords();
	chart[count]++;

	return count;
}
