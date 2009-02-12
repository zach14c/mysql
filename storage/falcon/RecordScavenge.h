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

#ifndef _RECORD_SCAVENGE_H_
#define _RECORD_SCAVENGE_H_

static const uint64 AGE_GROUPS = 100;
static const uint64 AGE_GROUPS_IN_CACHE = 50;

static const int CANNOT_SCAVENGE = 1;
static const int CAN_BE_RETIRED  = 2;
static const int CAN_BE_PRUNED   = 3;

class Database;
class Record;

class RecordScavenge
{
public:
	RecordScavenge(Database *db, uint64 generation, bool forceScavenge = false);
	~RecordScavenge(void);

	bool     canBeRetired(Record* record);
	Record*  inventoryRecord(Record* record);
	uint64   computeThreshold(uint64 spaceToRetire);
	void     print(void);

	Database	*database;
	TransId		oldestActiveTransaction;
	uint64		cycle;
	uint64		startingActiveMemory;
	uint64		prunedActiveMemory;
	uint64		retiredActiveMemory;

	time_t		scavengeStart;
	time_t		pruneStop;
	time_t		retireStop;

	uint64		baseGeneration;
	uint64		scavengeGeneration;

	// Results of Scavenging
	uint64		recordsPruned;
	uint64		spacePruned;
	uint64		recordsRetired;
	uint64		spaceRetired;
	uint64		recordsRemaining;
	uint64		spaceRemaining;

	// Results of the inventory
	uint64		totalRecords;
	uint64		totalRecordSpace;
	uint64		pruneableRecords;
	uint64		pruneableSpace;
	uint64		retireableRecords;
	uint64		retireableSpace;
	uint64		unScavengeableRecords;
	uint64		unScavengeableSpace;
	uint64		maxChainLength;
	uint64		ageGroups[AGE_GROUPS];
	uint64		veryOldRecords;
	uint64		veryOldRecordSpace;
	
	bool		forced;
};

#endif
