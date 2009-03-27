/* Copyright (C) 2009 Sun Microsystems, Inc.

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


#include "TransactionState.h"
#include "Interlock.h"
#include "Sync.h"

extern uint		falcon_lock_wait_timeout;

TransactionState::TransactionState()
{
	syncIsActive.setName("TransactionState::syncIaActive");
	pendingPageWrites = false;
	hasTransactionReference = false;
	waitingFor = NULL;
	commitId = 0;
	useCount = 1;
}


void TransactionState::waitForTransaction()
{
	Sync sync(&syncIsActive, "TransactionState::waitForTransaction");
	sync.lock(Shared, falcon_lock_wait_timeout * 1000);
}


void TransactionState::addRef()
{
	INTERLOCKED_INCREMENT(useCount);
}


void TransactionState::release()
{
	ASSERT(useCount > 0);

	if (INTERLOCKED_DECREMENT(useCount) == 0)
		delete this;
}

bool TransactionState::committedBefore(TransId transactionId)
{
	return commitId && commitId < transactionId;
}
