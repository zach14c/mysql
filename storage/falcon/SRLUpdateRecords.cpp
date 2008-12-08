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

#include <stdio.h>
#include "Engine.h"
#include "SRLUpdateRecords.h"
#include "SRLVersion.h"
#include "Stream.h"
#include "Table.h"
#include "SerialLogControl.h"
#include "SerialLogTransaction.h"
#include "Dbb.h"
#include "Transaction.h"
#include "RecordVersion.h"
#include "Log.h"
#include "Sync.h"
#include "SerialLogWindow.h"
#include "Format.h"

SRLUpdateRecords::SRLUpdateRecords(void)
{
}

SRLUpdateRecords::~SRLUpdateRecords(void)
{
}

void SRLUpdateRecords::chill(Transaction *transaction, RecordVersion *record, uint dataLength)
{
	// Record data has been written to the serial log, so release the data
	// buffer and set the state accordingly
	
	ASSERT(record->format);
	record->deleteData();
	record->state = recChilled;
	
	// Update transaction counter and chillPoint
	
	transaction->chillPoint = &record->nextInTrans;

	//ASSERT(transaction->totalRecordData >= dataLength);
	if (transaction->totalRecordData >= dataLength)
		transaction->totalRecordData -= dataLength;
	else
		transaction->totalRecordData = 0;
}

int SRLUpdateRecords::thaw(RecordVersion *record, bool *thawed)
{
	*thawed = false;
	
	// Nothing to do if record is no longer chilled
	
	if (record->state != recChilled)
		return record->size;

	// Find the window where the record is stored using the record offset, then
	// activate the window, reading from disk if necessary

	SerialLogWindow *window = log->findWindowGivenOffset(record->getVirtualOffset());
	
	// Return if the serial log window is no longer available

	if (!window)
		return 0;
		
	Sync sync(&log->syncWrite, "SRLUpdateRecords::thaw");
	sync.lock(Exclusive);
	
	// Return pointer to record data

	control->input = window->buffer + (record->getVirtualOffset() - window->virtualOffset);
	control->inputEnd = window->bufferEnd;
	
	// Get section id, record id and data length written. Input pointer will be at
	// beginning of record data.

	int tableSpaceId = 0;
	
	if (control->version >= srlVersion8)
		tableSpaceId = control->getInt();
		
	control->getInt();			// sectionId
	int recordNumber = control->getInt();
	int dataLength   = control->getInt();
	int bytesReallocated = 0;
	
	window->deactivateWindow();
	sync.unlock();
	
	// setRecordData() handles race conditions with an interlocked compare and exchange,
	// but check the state and record number anyway

	if (record->state == recChilled && recordNumber == record->recordNumber)
		bytesReallocated = record->setRecordData(control->input, dataLength);

	if (bytesReallocated > 0)
		{
		ASSERT(recordNumber == record->recordNumber);
		bytesReallocated = record->getEncodedSize();
		*thawed = true;

		if (log->chilledRecords > 0)
			log->chilledRecords--;
		
		if (log->chilledBytes > uint64(bytesReallocated))
			log->chilledBytes -= bytesReallocated;
		else
			log->chilledBytes = 0;
		}

	return bytesReallocated;
}

void SRLUpdateRecords::append(Transaction *transaction, RecordVersion *records, bool chillRecords)
{
	uint32 chilledRecordsWindow = 0;
	uint32 chilledBytesWindow   = 0;
	SerialLogTransaction *srlTrans = NULL;
	int savepointId;
	
	// Generate one serial log record per write window. To ensure that
	// chilled records are grouped by savepoint, start a new serial
	// log record for each savepoint. Several serial log records may
	// be generated for one savepoint.
	
	for (RecordVersion *record = records; record;)
		{
		START_RECORD(srlUpdateRecords, "SRLUpdateRecords::append");
		
		if (srlTrans == NULL)
			{
			srlTrans = log->getTransaction(transaction->transactionId);
			srlTrans->setTransaction(transaction);
			}

		putInt(transaction->transactionId);
		savepointId = (chillRecords) ? record->savePointId : 0;
		putInt(savepointId);
		UCHAR *lengthPtr = putFixedInt(0);
		UCHAR *start = log->writePtr;
		UCHAR *end = log->writeWarningTrack;
		
		chilledRecordsWindow = 0;
		chilledBytesWindow = 0;

		for (; record; record = record->nextInTrans)
			{
			// Skip lock records
			
			if (record->state == recLock)
				continue;
				
			// Skip chilled records, but advance the chillpoint
			
			if (record->state == recChilled)
				{
				transaction->chillPoint = &record->nextInTrans;
				continue;
				}
				
			// Skip record that is currently being used, but don't advance chillpoint
			
			if (record->state == recNoChill)
				continue;

			// If this is a different savepoint, break out of the inner loop
			// and start another serial log record
			
			if (chillRecords && record->savePointId != savepointId)
				break;
	
			Table *table = record->format->table;
			tableSpaceId = table->dbb->tableSpaceId;
			Stream stream;
			
			// A non-zero virtual offset indicates that the record was previously
			// chilled and thawed and is already in the serial log.
			//
			// If this record is being re-chilled, then there is no need to get
			// the record data or set the virtual offset.
			//
			// If this record is being committed, then there is nothing to do.

			if (record->virtualOffset != 0)
				{
				if (chillRecords && record->state != recDeleted)
					{
					int chillBytes = record->getEncodedSize();

					chill(transaction, record, chillBytes);
					
					log->chilledRecords++;
					log->chilledBytes += chillBytes;
					
					ASSERT(transaction->thawedRecords > 0);

					if (transaction->thawedRecords)
						transaction->thawedRecords--;
					}
				else
					{
					// Record is already in serial log
					}

				continue;
				}
			
			// Load the record data into a stream

			if (record->hasRecord(false))
				{
				if (!record->getRecord(&stream))
					continue;
				}
			else
				ASSERT(record->state == recDeleted);
			
			// Ensure record fits within current window

			if (log->writePtr + 
				 byteCount(tableSpaceId) + 
				 byteCount(table->dataSectionId) + 
				 byteCount(record->recordNumber) + 
				 byteCount(stream.totalLength) + stream.totalLength >= end)
				break;
			
			ASSERT(record->recordNumber >= 0);
			ASSERT(log->writePtr > (UCHAR *)log->writeWindow->buffer);
			
			// Set the virtual offset of the record in the serial log

			record->setVirtualOffset(log->writeWindow->currentLength + log->writeWindow->virtualOffset);
			uint32 sectionId = table->dataSectionId;
			log->updateSectionUseVector(sectionId, tableSpaceId, 1);
			
			putInt(tableSpaceId);
			putInt(record->getPriorVersion() ? sectionId : -(int) sectionId - 1);
			putInt(record->recordNumber);
			putStream(&stream);
			
			if (chillRecords && record->state != recDeleted)
				{
				chill(transaction, record, stream.totalLength);
				chilledRecordsWindow++;
				chilledBytesWindow += stream.totalLength;
				}
			} // next record version
		
		int len = (int) (log->writePtr - start);
		
		// The length field is 0, update if necessary
		
		if (len > 0)
			putFixedInt(len, lengthPtr);
		
		// Flush record data, if any, and force the creation of a new serial log window
		
		if (record && len > 0)
			log->flush(true, 0, &sync);
		else
			sync.unlock();
			
		if (chillRecords)
			{
			log->chilledRecords += chilledRecordsWindow;
			log->chilledBytes   += chilledBytesWindow;
			transaction->chilledRecords += chilledRecordsWindow;
			transaction->chilledBytes += chilledBytesWindow;
//			uint32 windowNumber = (uint32)log->writeWindow->virtualOffset / SRL_WINDOW_SIZE;
			}
		} // next serial log record and write window
}

void SRLUpdateRecords::read(void)
{
	transactionId = getInt();

	if (control->version >= srlVersion13)
		savepointId = getInt();
	else
		savepointId = 0;

	dataLength = getInt();
	data = getData(dataLength);
}

void SRLUpdateRecords::redo(void)
{
	SerialLogTransaction *transaction = control->getTransaction(transactionId);

	if (savepointId != 0 && transaction->isRolledBackSavepoint(savepointId))
		return;
	
	if (transaction->state == sltCommitted)
		for (const UCHAR *p = data, *end = data + dataLength; p < end;)
			{
			if (control->version >= srlVersion8)
				tableSpaceId = getInt(&p);
			else
				tableSpaceId = 0;
		
			int id = getInt(&p);
			uint sectionId = (id >= 0) ? id : -id - 1;
			int recordNumber = getInt(&p);
			int length = getInt(&p);
			log->updateSectionUseVector(sectionId, tableSpaceId, -1);

			if (log->traceRecord && recordNumber == log->traceRecord)
				print();
					
			if (log->bumpSectionIncarnation(sectionId, tableSpaceId, objInUse))
				{
				Dbb *dbb = log->getDbb(tableSpaceId);
				
				if (length)
					{
					if (id < 0)
						dbb->reInsertStub(sectionId, recordNumber, transactionId);
						
					Stream stream;
					stream.putSegment(length, (const char*) p, false);
					dbb->updateRecord(sectionId, recordNumber, &stream, transactionId, false);
					}
				else
					dbb->updateRecord(sectionId, recordNumber, NULL, transactionId, false);
				}
			
			p += length;
			}
	else
		pass1();
}

void SRLUpdateRecords::pass1(void)
{
	control->getTransaction(transactionId);

	for (const UCHAR *p = data, *end = data + dataLength; p < end;)
		{
		if (control->version >= srlVersion8)
			tableSpaceId = getInt(&p);
		else
			tableSpaceId = 0;
			
		int id = getInt(&p);
		uint sectionId = (id >= 0) ? id : -id - 1;
		getInt(&p);			// recordNumber
		int length = getInt(&p);
		log->bumpSectionIncarnation(sectionId, tableSpaceId, objInUse);
		p += length;
		}
}

void SRLUpdateRecords::pass2(void)
{
	pass1();
}

void SRLUpdateRecords::commit(void)
{
	if (savepointId != 0)
		{
		SerialLogTransaction *transaction = log->getTransaction(transactionId);
		
		if (transaction->isRolledBackSavepoint(savepointId))
			return;
		}

	Sync sync(&log->syncSections, "SRLUpdateRecords::commit");
	sync.lock(Shared);
	
	for (const UCHAR *p = data, *end = data + dataLength; p < end;)
		{
		if (control->version >= srlVersion8)
			tableSpaceId = getInt(&p);
		else
			tableSpaceId = 0;
			
		int id = getInt(&p);
		uint sectionId = (id >= 0) ? id : -id - 1;
		int recordNumber = getInt(&p);
		int length = getInt(&p);
		log->updateSectionUseVector(sectionId, tableSpaceId, -1);
		
		if (log->isSectionActive(sectionId, tableSpaceId))
			{
			Dbb *dbb = log->getDbb(tableSpaceId);

			if (length)
				{
				Stream stream;
				stream.putSegment(length, (const char*) p, false);
				dbb->updateRecord(sectionId, recordNumber, &stream, transactionId, false);
				}
			else
				dbb->updateRecord(sectionId, recordNumber, NULL, transactionId, false);
			}
		
		p += length;
		}
}

void SRLUpdateRecords::print(void)
{
	logPrint("UpdateRecords: transaction %d, savepointId %d, length %d\n", transactionId, savepointId, dataLength);
	
	for (const UCHAR *p = data, *end = data + dataLength; p < end;)
		{
		if (control->version >= srlVersion8)
			tableSpaceId = getInt(&p);
		else
			tableSpaceId = 0;

		int id = getInt(&p);
		uint sectionId = (id >= 0) ? id : -id - 1;
		int recordNumber = getInt(&p);
		int length = getInt(&p);
		char temp[40];
		Log::debug("   rec %d, len %d to section %d/%d %s\n", 
					recordNumber, length, sectionId, tableSpaceId, format(length, p, sizeof(temp), temp));
		p += length;
		}
}
