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
#include "SRLOverflowPages.h"
#include "Bitmap.h"
#include "SerialLogControl.h"
#include "SerialLogTransaction.h"
#include "Dbb.h"

SRLOverflowPages::SRLOverflowPages(void)
{
}

SRLOverflowPages::~SRLOverflowPages(void)
{
}

void SRLOverflowPages::append(Dbb *dbb, Bitmap* pageNumbers, bool earlyWriteFlag)
{
	for (int pageNumber = 0; pageNumber >= 0;)
		{
		START_RECORD(srlOverflowPages, "SRLOverflowPages::append");
		putInt(dbb->tableSpaceId);
		putInt(earlyWriteFlag);
		UCHAR *lengthPtr = putFixedInt(0);
		UCHAR *start = log->writePtr;
		UCHAR *end = log->writeWarningTrack;

		for (; (pageNumber = pageNumbers->nextSet(pageNumber)) >= 0; ++pageNumber)
			{
			if (log->writePtr + 5 >= end)
				break;
			
			putInt(pageNumber);
			}
		
		int len = (int) (log->writePtr - start);
		putFixedInt(len, lengthPtr);
		
		if (pageNumber >= 0)
			log->flush(true, 0, &sync);
		else
			sync.unlock();
		}
}

void SRLOverflowPages::read(void)
{
	if (control->version >= srlVersion8)
		tableSpaceId = getInt();
	else
		tableSpaceId = 0;
	
	if (control->version >=srlVersion20)
		earlyWrite = getInt();
	else
		earlyWrite = 0;

	dataLength = getInt();
	data = getData(dataLength);
}

void SRLOverflowPages::pass1(void)
{
	for (const UCHAR *p = data, *end = data + dataLength; p < end;)
		{
		int pageNumber = getInt(&p);
		
		if (log->tracePage == pageNumber)
			print();
		
		log->bumpPageIncarnation(pageNumber, tableSpaceId, objInUse);
		}
}

void SRLOverflowPages::pass2(void)
{
	for (const UCHAR *p = data, *end = data + dataLength; p < end;)
		{
		int pageNumber = getInt(&p);
		
		if (log->tracePage == pageNumber)
			print();
		
		if (log->bumpPageIncarnation(pageNumber, tableSpaceId, objInUse))
			{
			bool isPageValid = false;

			if (earlyWrite)
				{
				SerialLogTransaction *transaction = log->findTransaction(transactionId);

				if(transaction && transaction->state == sltCommitted)
					// Page was flushed at commit
					isPageValid = true;
				}

			if (isPageValid)
				{
				log->setOverflowPageValid(pageNumber, tableSpaceId);
				}
			else
				{
				log->setOverflowPageInvalid(pageNumber, tableSpaceId);
				// Normal overflow pages  are always recreated in recovery,
				// and we will deleted them here. Also delete uncommitted blob 
				// pages.

				// However, with older serial logs earlyWrite flag was not available,
				// and we cannot tell a normal page from a blob page.
				// In this case, keep the page - it might be a part of a valid blob.
				if (control->version >= srlVersion20)
					{
					log->redoFreePage(pageNumber, tableSpaceId);
					}
				}
			}
		}
}

void SRLOverflowPages::redo(void)
{
	for (const UCHAR *p = data, *end = data + dataLength; p < end;)
		{
		int pageNumber = getInt(&p);
		
		if (log->tracePage == pageNumber)
			print();
		
		log->bumpPageIncarnation(pageNumber, tableSpaceId, objInUse);
		}
}

void SRLOverflowPages::print(void)
{
	logPrint("Overflow Pages length %d\n", dataLength);
}
