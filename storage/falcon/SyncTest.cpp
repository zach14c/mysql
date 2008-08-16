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
#include "SyncTest.h"
#include "Sync.h"
#include "Thread.h"
#include "Threads.h"

#ifdef _DEBUG
#undef THIS_FILE
static const char THIS_FILE[]=__FILE__;
#endif

SyncTest::SyncTest(void) : Thread("SyncTest")
{
	threads = NULL;
}

SyncTest::~SyncTest(void)
{
	useCount = 0;
	delete [] threads;
}

void SyncTest::test()
{
	if (!threads)
		threads = new SyncTest[MAX_THREADS];
	
	Sync sync(&starter, "SyncTest::test");
	Threads *threadBarn = new Threads(NULL, MAX_THREADS);
	int grandTotal = 0;
		
	for (int n = 1; n <= MAX_THREADS; ++n)
		{
		sync.lock(Exclusive);
		int collisions = syncObject.getCollisionCount();
		stop = false;
		int thd;
		
		for (thd = 0; thd < n; ++thd)
			{
			SyncTest *thread = threads + thd;
			thread->parent = this;
			thread->ready = false;
			threadBarn->start("", testThread, thread);
			}
		
		for(;;)
			{
			bool waiting = false;
			
			for (thd = 0; thd < n; ++thd)
				if (!threads[thd].ready)
					{
					waiting = true;
					break;
					}
			
			if (!waiting)
				break;
				
			Thread::sleep(100);
			}
			
		sync.unlock();
		Thread::sleep(1000);
		stop = true;
		threadBarn->waitForAll();
		int total = 0;
		
		for (thd = 0; thd < n; ++thd)
			total += threads[thd].count;
		
		if (n != 1)
			grandTotal += total;
			
		printf("%d threads, %s cycles, %s collisions\n", n, 
				(const char*) format(total), 
				(const char*) format(syncObject.getCollisionCount() - collisions));

		/***
		for (thd = 0; thd < n; ++thd)
			printf(" %d", threads[thd].count);
					
		printf("\n");
		***/
		}
	
	printf ("Average cycles %s\n", (const char*) format(grandTotal / (MAX_THREADS - 1)));
	threadBarn->shutdownAll();
	threadBarn->waitForAll();
	threadBarn->release();
}

void SyncTest::testThread(void* parameter)
{
	((SyncTest*) parameter)->testThread();
}

void SyncTest::testThread(void)
{
	count = 0;
	Sync syncStart(&starter, "SyncTest::testThread(1)");
	ready = true;
	syncStart.lock(Shared);
	Sync sync(&parent->syncObject, "SyncTest::testThread(2)");
	
	while (!parent->stop)
		{
		++count;
		sync.lock(Shared);
		sync.unlock();
		}
}

JString SyncTest::format(long num)
{
	char temp[32];
	long number = num;
	char *p = temp + sizeof(temp);
	*--p = 0;
	
	if (number == 0)
		{
		*--p = '0';
		
		return p;
		}
	
	bool neg = false;
	
	if (number < 0)
		{
		neg = true;
		number = -number;
		}
		
	for (int n = 1; number; ++n)
		{
		*--p = (char) (number % 10) + '0';
		number /= 10;
				
		if (number && (n % 3 == 0))
			*--p = ',';
		}
	
	if (neg)
		*--p = '-';
		
	return JString(p);
}
