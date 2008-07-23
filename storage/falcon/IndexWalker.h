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

#ifndef _INDEX_WALKER_H_
#define _INDEX_WALKER_H_

#include "IndexKey.h"

class Index;
class Transaction;
class Record;
class Table;

class IndexWalker
{
public:
	IndexWalker(Index *index, Transaction *transaction, int flags);
	virtual ~IndexWalker(void);
	
	virtual Record*		getNext(bool lockForUpdate);

	Record*				getValidatedRecord(int32 recordId, bool lockForUpdate);
	int					compare(IndexWalker* node1, IndexWalker* node2);
	void				addWalker(IndexWalker* walker);
	bool				insert(IndexWalker* node);
	void				rotateLeft(void);
	void				rotateRight(void);
	void				rebalance(void);
	bool				rebalanceDelete();
	IndexWalker*		getSuccessor(IndexWalker** parentPointer, bool *shallower);
	void				remove(void);
	void				rebalanceUpward(int delta);
	// debugging routines 
	int					validate(void);
	void				corrupt(const char *text);
	
	Index			*index;
	Transaction		*transaction;
	Record			*currentRecord;
	Table			*table;
	IndexWalker		*next;
	IndexWalker		*higher;
	IndexWalker		*lower;
	IndexWalker		*parent;
	UCHAR			*key;
	uint			keyLength;
	int32			recordNumber;
	int				balance;
	int				searchFlags;
	bool			first;
};

#endif
