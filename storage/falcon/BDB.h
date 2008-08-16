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

// Bdb.h: interface for the Bdb class.
//
//////////////////////////////////////////////////////////////////////

#if !defined(AFX_BDB_H__6A019C1C_A340_11D2_AB5A_0000C01D2301__INCLUDED_)
#define AFX_BDB_H__6A019C1C_A340_11D2_AB5A_0000C01D2301__INCLUDED_

#if _MSC_VER >= 1000
#pragma once
#endif // _MSC_VER >= 1000

#include "SyncObject.h"

//#define COLLECT_BDB_HISTORY
#if defined _DEBUG  && defined COLLECT_BDB_HISTORY
	#define ADD_HISTORY          +1, THIS_FILE, __LINE__
	#define COMMA_ADD_HISTORY   ,+1, THIS_FILE, __LINE__
	#define REL_HISTORY          -1, THIS_FILE, __LINE__
	#define BDB_HISTORY(_bdb_)   {if (_bdb_) (_bdb_)->addHistory(0, THIS_FILE, __LINE__);}
	#define MAX_BDB_HISTORY 100
	#define BDB_HISTORY_FILE_LEN 16
	struct bdb_history
	{
		unsigned long	threadId;
		LockType	lockType;
		short		useCount;
		short		delta;
		short		line;
		char		file[BDB_HISTORY_FILE_LEN];
	};
#else
	#define HISTORY
	#define ADD_HISTORY
	#define COMMA_ADD_HISTORY
	#define REL_HISTORY
	#define BDB_HISTORY(_bdb_)  {}
#endif

class Page;
class Cache;
class Dbb;
class Thread;

class Bdb
{
public:
	void	setWriter();
	void	decrementUseCount();
	void	incrementUseCount();
	void	downGrade (LockType lockType);
	void	addRef(LockType lType);
	void	release();
	void	mark(TransId transId);
	void	setPageHeader(short type);
	Bdb();
	~Bdb();
#ifdef COLLECT_BDB_HISTORY
	void	ShowAllHistory(void);
	void	ShowHistory(void);
	void	initHistory();
	void	addHistory(int delta, const char *file, int line);
	void	addRef(LockType lType, int category, const char *file, int line);
	void	release(int category, const char *file, int line);
	void	incrementUseCount(int category, const char *file, int line);
	void	decrementUseCount(int category, const char *file, int line);
#endif

	Cache			*cache;
	Dbb				*dbb;
	int32			pageNumber;
	TransId			transactionId;
	uint64			age;
	Page			*buffer;
	Bdb				*prior;		/* position in LRU que */
	Bdb				*next;
	Bdb				*hash;		/* hash collision */
	Bdb				*nextDirty;
	Bdb				*priorDirty;
	Bdb				*ioThreadNext;
	Thread			*markingThread;
	SyncObject		syncObject;
	SyncObject		syncWrite;
	time_t			lastMark;
	LockType		lockType;
	bool			flushIt;				// PageWriter wants to hear about this
	bool			isDirty;
	bool			isRegistered;			// Register with PageWrite on next release
	volatile INTERLOCK_TYPE	useCount;

#ifdef COLLECT_BDB_HISTORY
	SyncObject	syncHistory;
	uint initCount;
	uint historyCount;
	struct bdb_history		history[MAX_BDB_HISTORY];
#endif
};

#endif // !defined(AFX_BDB_H__6A019C1C_A340_11D2_AB5A_0000C01D2301__INCLUDED_)
