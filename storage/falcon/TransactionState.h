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

#if !defined(TRANSACTION_STATE_H)
#define TRANSACTION_STATE_H

#include "Engine.h"
#include "SyncObject.h"

// Transaction States

enum State {
	Active,					// 0
	Limbo,					// 1
	Committed,				// 2
	RolledBack,				// 3

	// The following are 'relative states'.  See getRelativeState()

	Us,						// 4
	CommittedVisible,		// 5
	CommittedInvisible,		// 6
	WasActive,				// 7
	Deadlock,				// 8
	
	// And the remaining are for transactions pending reuse
	
	Available,				// 9
	Initializing			// 10
	};


// TransactionState stores the main state information for a Transaction.
// The reason for having this as a separate class instead of having it
// stored in the Transaction object is that we want to be able to purge
// transaction objects earlier than when record versions are scavanged. 
// To be able to do that we let the TransactionState object live as long
// as there is RecordVersions refering to it while we purge the Transaction 
// objects earlier. 

class TransactionState
{
public:
	TransactionState();

	void		addRef();
	void		release();
	void		waitForTransaction();
	bool		committedBefore(TransId transactionId);
	
	inline bool	isActive() const
	    {
		return state == Active || state == Limbo;
	    }

	inline bool	isCommitted() const
	    {
		return state == Active || state == Limbo;
	    }


public:
	TransId			transactionId;       // used also as startEvent by dep.mgr.
	TransId			commitId;            // used as commitEvent by dep.mgr.
	volatile		INTERLOCK_TYPE state;
	SyncObject 		syncIsActive;
	bool			pendingPageWrites;
	
	volatile TransactionState* waitingFor; // Used for deadlock detection

private:
	volatile INTERLOCK_TYPE useCount;
};

#endif
