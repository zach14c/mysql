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

#define RESET_PARENT(newChild)		\
	if (parent->lower == this)		\
		parent->lower = newChild;	 \
	else							\
		parent->higher = newChild;
		

IndexWalker::IndexWalker(Index *idx, Transaction *trans, int flags)
{
	index = idx;
	table = index->table;
	transaction = trans;
	searchFlags = flags;
	currentRecord = NULL;
	parent = NULL;
	next = NULL;
	first = true;
	balance = 0;
	higher = NULL;
	lower = NULL;
	lastRecordNumber = 0;
	firstRecord = true;
}

IndexWalker::~IndexWalker(void)
{
	if (next)
		delete next;
}

Record* IndexWalker::getNext(bool lockForUpdate)
{
	// If this is the first time in, prime the data structures
	
	if (first)
		{
		first = false;
		higher = NULL;
		lower = NULL;
		balance = 0;

		for (IndexWalker *walker = next; walker; walker = walker->next)
			while (walker->getNext(lockForUpdate))
				{
				walker->higher = NULL;
				walker->lower = NULL;
				walker->balance = 0;
			
				if (higher)
					{
					if (higher->insert(walker))
						break;
					}
				else
					{
					higher = walker;
					walker->parent = this;
					walker->balance = 0;
					
					break;
					}
				}
		}

	for (IndexWalker *walker; (walker = higher);)
		{
		// Find the walker with the lowest key value
		
		while (walker->lower)
			walker = walker->lower;
		
		// Save the record, then take the walker out of the tree
		
		Record *record = walker->currentRecord;
		walker->remove();
		
		// Find the next record for the walker.  If successful, put it back in the tree
		
		while (walker->getNext(lockForUpdate))
			{
			walker->higher = NULL;
			walker->lower = NULL;
			walker->balance = 0;
			
			if (higher)
				{
				if (higher->insert(walker))
					break;
				}
			else
				{
				higher = walker;
				walker->parent = this;
				walker->balance = 0;
				
				break;
				}
			}
		
		return record;
		}
		
	return NULL;
}

Record* IndexWalker::getValidatedRecord(int32 recordId, bool lockForUpdate)
{
	// If this is the same recordId as the last record we returned,
	// then either we've got a duplicate copy because a deferred
	// index has been merged with the main index and we read both
	// or we're looking at a version we don't care about.

	if (firstRecord)
		firstRecord = false;
	else if (recordId == lastRecordNumber)
		return NULL;

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
	
	if (recordKey.keyLength != keyLength ||
		memcmp(recordKey.key, key, keyLength) != 0)
		{
		record->release();
		
		return NULL;
		}

	// remember this record

	lastRecordNumber = recordId;

	return record;
}

void IndexWalker::addWalker(IndexWalker* walker)
{
	walker->next = next;
	next = walker;
}

bool IndexWalker::insert(IndexWalker* newNode)
{
	// Find insertion point and insert new node as leaf
	
	IndexWalker *node;
//	validate();

	for (node = this; node;)
		{
		int comparison = compare(newNode, node);

		if (comparison < 0)
			{
			if (node->lower)
				node = node->lower;
			else
				{
				node->lower = newNode;
				--node->balance;
				
				break;
				}
			}
		else if (comparison > 0)
			{
			if (node->higher)
				node = node->higher;
			else
				{
				node->higher = newNode;
				++node->balance;
				
				break;
				}
			}
		else
			{
			//printf("Duplicate %d\n", value);

//			validate();
			return false;
			}
		}
	
	// New node is inserted.  Balance tree upwards
	
	newNode->parent = node;

	for (IndexWalker *nodeParent; node->balance && (nodeParent = node->parent) && nodeParent->parent; node = nodeParent)
		{
		if (nodeParent->lower == node)
			{
			if (--nodeParent->balance < -1)
				{
				nodeParent->rebalance();
				break;
				}
			}
		else
			{
			if (++nodeParent->balance > +1)
				{
				nodeParent->rebalance();
				break;
				}
			}
//		validate();
		}
	
	return true;
}

/*
 *  Rebalance an unbalanced tree.  This is used for insert
 */
 
void IndexWalker::rebalance(void)
{
	if (balance > 1)
		{
		if (higher->balance < 0)
			higher->rotateRight();
					
		rotateLeft();
//		validate();
		}
	else if (balance < 1)
		{
		if (lower->balance > 0)
			lower->rotateLeft();
			
		rotateRight();
//		validate();
        }
}

/*
 * Rebalance an unbalanced tree taking node of whether the tree got shallower.  This
 * is used for deletion.
 */
 
bool IndexWalker::rebalanceDelete()
{
	if (balance > 1)
		{
		if (higher->balance < 0)
			{
			higher->rotateRight();
			rotateLeft();
//			validate();

			return true;
			}
					
		rotateLeft();
//		validate();

		return parent->balance == 0;
		}
		
	if (balance < 1)
		{
		if (lower->balance > 0)
			{
			lower->rotateLeft();
			rotateRight();

			return true;
			}
			
		rotateRight();

		return parent->balance == 0;
		}
	
	return false;
}



/*
	Swap nodes A and B in a tree
	
			A					B
		   / \                 / \
		  a   B      =>       A   C
		     / \			 / \
		    b   c			a   b
 */
 
void IndexWalker::rotateLeft(void)
{
	IndexWalker *root = higher;
	
	if ( (higher = higher->lower) )
		higher->parent = this;

	root->lower = this;
	balance -= (1 + MAX(root->balance, 0));
//	validate ();
	root->balance -= (1 - MIN(balance, 0));
	RESET_PARENT(root);
	root->parent = parent;
	parent = root;

//	validate ();
}

/*
	Swap nodes B and A in a tree
	
			B                  A
		   / \                / \
		  A   b    =>        a   B            
		 / \                    / \
		a   c                  c   b
 */
 
void IndexWalker::rotateRight(void)
{
	IndexWalker *root = lower;
	
	if ( (lower = lower->higher) )
		lower->parent = this;
		
	root->higher = this;
	balance += (1 - MIN(root->balance, 0));

//	validate ();
	root->balance += (1 + MAX(balance, 0));
	RESET_PARENT(root);
	root->parent = parent;
	parent = root;
//	validate ();
}

/*
 * Look for a success node for a node being remove with two children.  This
 * requires that we find the next higher value, which is also the lowest value
 * in the deletion nodes right sub-tree.
 */
 
IndexWalker* IndexWalker::getSuccessor(IndexWalker** parentPointer, bool *shallower)
{
	// If there's a lower node, recurse down it rebalancing
	// if the subtree gets shallower
	
	if (lower)
		{
		int was = balance;
		IndexWalker *node = lower->getSuccessor(&lower, shallower);

		// If we got shallower, adjust balance and rebalance if necessary

		if (*shallower)
			{
			if (++balance > 1)
				*shallower = rebalanceDelete();
			else if (!was && (*parentPointer)->balance)
				*shallower = false;
			}

//		validate();	
		return node;
		}
	
	// We're the bottom.  Remove us from our parent and we're done.
	
	RESET_PARENT(higher);
	
	if (higher)
		higher->parent = parent;
		
	*shallower = true;
//	validate();

	return this;
}

int IndexWalker::compare(IndexWalker* node1, IndexWalker* node2)
{
	int length = MIN(node1->keyLength, node2->keyLength);
	
	for (UCHAR *key1 = node1->key, *key2 = node2->key, *end = key1 + length; key1 < end;)
		{
		int c = *key1++ - * key2++;
		
		if (c)
			return c;
		}

	int c = node1->keyLength - node2->keyLength;
	
	if (c)
		return c;
		
	return node1->recordNumber - node2->recordNumber;
}

void IndexWalker::remove(void)
{
	// There are three cases a) No children, b) One child, c) Two children

	if (!lower || !higher)
		{
		IndexWalker *next = (lower) ? lower : higher;
		
		if (next)
			next->parent = parent;
		
		if (parent->lower == this)
			{
			parent->lower = next;
			parent->rebalanceUpward(+1);
			}
		else
			{
			parent->higher = next;
			parent->rebalanceUpward(-1);
			}	
		
		return;	
		}

	// We're an equality with two children.  Bummer.  Find successor node (next higher value)
	// and swap it into our place
	
	bool shallower;
	IndexWalker *node = higher->getSuccessor(&higher, &shallower);
	
	if ( (node->lower = lower) )
		lower->parent = node;
		
	if ( (node->higher = higher) )
		higher->parent = node;

	node->balance = balance;
	node->parent = parent;
	RESET_PARENT(node);
	
	// If we got shallower, adjust balance and rebalance if necessary

	if (shallower)
		node->rebalanceUpward(-1);

//	validate();
}

void IndexWalker::rebalanceUpward(int delta)
{
	IndexWalker *node = this;

	for (;;)
		{
		IndexWalker *nodeParent = node->parent;
		
		if (!nodeParent)
			return;

		int parentDelta = (nodeParent->lower == node) ? 1 : -1;
		node->balance += delta;

		if (node->balance == delta)
			break;

		if (node->balance > 1 || node->balance < -1)
			if (!node->rebalanceDelete())
				break;
		
		delta = parentDelta;			
		node = nodeParent;
		}

//	validate();
}

int IndexWalker::validate(void)
{
	int rightDepth = 0;
	int leftDepth = 0;
	
	if (lower)
		{			
		if (lower->parent != this)
			corrupt("bad parent");
		
		leftDepth = lower->validate();
		}
	
	if (higher)
		{			
		if (higher->parent != this)
			corrupt("bad right parent");
		
		rightDepth = higher->validate();
		}
	
	if (parent && balance != rightDepth - leftDepth)
		corrupt("bad balance");
		
	return MAX(rightDepth, leftDepth) + 1;
}

void IndexWalker::corrupt(const char *text)
{
	ASSERT(false);
}
