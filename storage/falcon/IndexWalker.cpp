/* Copyright (C) 2008 MySQL AB

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

#include "Engine.h"
#include "IndexWalker.h"
#include "Record.h"
#include "Table.h"
#include "SQLError.h"

IndexWalker::IndexWalker(Index *idx, Transaction *trans, int flags)
{
	index = idx;
	table = index->table;
	transaction = trans;
	searchFlags = flags;
	currentRecord = NULL;
	first = true;
}

IndexWalker::~IndexWalker(void)
{
}

Record* IndexWalker::getNext(bool lockForUpdate)
{
	return NULL;
}

Record* IndexWalker::getValidatedRecord(int32 recordId, bool lockForUpdate)
{
	// Fetch record.  If it doesn't exist, that's ok.
	
	Record *candidate = table->fetch(recordId);

	if (!candidate)
		return NULL;
	
	// Get the correct version.  If this is select for update, get a lock record
			
	Record *record = (lockForUpdate) 
				    ? table->fetchForUpdate(transaction, candidate, true)
				    : candidate->fetchVersion(transaction);
	
	if (!record)
		{
		if (!lockForUpdate)
			candidate->release();
		
		return NULL;
		}
	
	// If we have a different record version, release the original
	
	if (!lockForUpdate && candidate != record)
		{
		record->addRef();
		candidate->release();
		}
	
	// Compute record key and compare against index key.  If there' different, punt
	
	IndexKey recordKey;
	index->makeKey(record, &recordKey);
	
	if (recordKey.keyLength != key.keyLength ||
		memcmp(recordKey.key, key.key, key.keyLength) != 0)
		{
		record->release();
		
		return NULL;
		}
	
	return record;
}
