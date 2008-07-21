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

#include <memory.h>
#include "Engine.h"
#include "WalkIndex.h"
#include "Index.h"
#include "Transaction.h"
#include "Database.h"
#include "Dbb.h"
#include "WalkIndex.h"
#include "WalkIndex.h"
#include "IndexRootPage.h"

#ifdef _DEBUG
#undef THIS_FILE
static const char THIS_FILE[]=__FILE__;
#endif

//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////

WalkIndex::WalkIndex(Index *index, Transaction *transaction, int flags, IndexKey *lower, IndexKey *upper) : IndexWalker(index, transaction, flags)
{
	if (lower)
		lowerBound.setKey(lower);
	
	if (upper)
		upperBound.setKey(upper);
	
	nodes = new UCHAR[transaction->database->dbb->pageSize];
	key = indexKey.key;
}

WalkIndex::~WalkIndex(void)
{
	delete [] nodes;
}

void WalkIndex::setNodes(int32 nextIndexPage, int length, Btn* stuff)
{
	memcpy(nodes, stuff, length);
	endNodes = (Btn*) (nodes + length);
	node.parseNode((Btn*) nodes, endNodes);
	nextPage = nextIndexPage;
}

Record* WalkIndex::getNext(bool lockForUpdate)
{
	for (;;)
		{
		int32 recordNumber = getNextNode();
		
		if (recordNumber < 0)
			{
			currentRecord = NULL;
			
			return NULL;
			}
		
		keyLength = indexKey.keyLength;
		
		if ( (currentRecord = getValidatedRecord(recordNumber, lockForUpdate)) )
			return currentRecord;
		}
}

int32 WalkIndex::getNextNode(void)
{
	for (;; first = true)
		{
		if (first)
			{
			first = false;
			recordNumber = node.getNumber();
			
			if (recordNumber >= 0)
				return recordNumber;
			else if (recordNumber == END_LEVEL)
				return -1;
			}
			
		node.getNext(endNodes);
		
		if (node.node < endNodes)
			{
			recordNumber = node.getNumber();
			node.expandKey(&indexKey);
			
			if (recordNumber >= 0)
				return recordNumber;
			}
		
		if (nextPage == 0)
			return -1;
			
		IndexRootPage::repositionIndex(index->dbb, index->indexId, this);
		}
	
}
