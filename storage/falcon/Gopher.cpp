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

#include "Engine.h"
#include "Gopher.h"
#include "SerialLog.h"
#include "Sync.h"
#include "Thread.h"
#include "Threads.h"
#include "SerialLogTransaction.h"
#include "Database.h"

Gopher::Gopher(SerialLog *serialLog)
{
	log = serialLog;
	workerThread = NULL;
}

Gopher::~Gopher(void)
{
}

void Gopher::gopherThread(void* arg)
{
	((Gopher*) arg)->gopherThread();
}

void Gopher::gopherThread(void)
{
	Sync deadMan(&log->syncGopher, "Gopher::gopherThread");
	deadMan.lock(Shared);
	workerThread = Thread::getThread("Gopher::gopherThread");
	active = true;
	Sync syncPending (&log->pending.syncObject, "Gopher::gopherThread pending");
	syncPending.lock(Exclusive);
	
	while (!workerThread->shutdownInProgress && !log->finishing)
		{
		if (!log->pending.first || !log->pending.first->isRipe())
			{
			if (log->blocking)
				log->unblockUpdates();

			syncPending.unlock();
			active = false;
			workerThread->sleep();
			active = true;
			syncPending.lock(Exclusive);

			continue;
			}
		
		SerialLogTransaction *transaction = log->pending.first;
		log->pending.remove(transaction);

		setConcurrency(&syncPending, transaction->allowConcurrentGophers);
		syncPending.unlock();

		transaction->doAction();

		syncPending.lock(Exclusive);
		releaseConcurrency(&syncPending, transaction->allowConcurrentGophers);

		log->inactions.append(transaction);
		
		if (log->pending.count > log->maxTransactions && !log->blocking)
			log->blockUpdates();
		}

	active = false;
	workerThread = NULL;
}

void Gopher::setConcurrency(Sync *syncPending, bool allowConcurrentGophers)
{
	// Assume that syncPending is locked exclusively.

	if (allowConcurrentGophers)
		{
		while (log->serializeGophers < 0)
			{
			syncPending->unlock();
			workerThread->sleep(10);
			syncPending->lock(Exclusive);
			}

		log->serializeGophers++;
		}
	else
		{
		log->wantToSerializeGophers++;
		while (log->serializeGophers)
			{
			syncPending->unlock();
			workerThread->sleep(10);
			syncPending->lock(Exclusive);
			}

		log->serializeGophers = -1;
		}
}

void Gopher::releaseConcurrency(Sync *syncPending, bool allowConcurrentGophers)
{
	if (allowConcurrentGophers)
		{
		ASSERT(log->serializeGophers > 0);
		log->serializeGophers--;
		}
	else
		{
		ASSERT(log->serializeGophers == -1);
		log->wantToSerializeGophers--;
		log->serializeGophers = 0;
		}

	// If there is another thread that needs to serialize the gophers, 
	// wait here until it is done.

	while (log->wantToSerializeGophers)
		{
		syncPending->unlock();
		workerThread->sleep(10);
		syncPending->lock(Exclusive);
		}
}

void Gopher::start(void)
{
	log->database->threads->start("SerialLog::start", gopherThread, this);
}

void Gopher::shutdown(void)
{
	if (workerThread)
		workerThread->shutdown();
}

void Gopher::wakeup(void)
{
	if (workerThread)
		workerThread->wake();
}
