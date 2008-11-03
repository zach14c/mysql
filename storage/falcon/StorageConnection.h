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

#ifndef _STORAGE_CONNECTION_H_
#define _STORAGE_CONNECTION_H_

#include "JString.h"

static const int VALIDATE_REPAIR	= 1;

class StorageConnection;
class StorageTable;
class StorageDatabase;
class Database;
class THD;
class Table;
class Stream;
class Connection;
class THD;
class StorageTableShare;
class StorageHandler;
class SQLException;
class Transaction;

class StorageConnection
{
public:
	//StorageConnection(StorageHandler *handler, const char* path, THD *mySqlThread);
	StorageConnection(StorageHandler* handler, StorageDatabase* db, THD* mySqlThd, int mySqlThdId);
	virtual ~StorageConnection(void);
	
	//static StorageConnection*	getStorageConnection(const char* path, THD *mySqlThread, OpenOption create);
	static const char*			findNameSegment(const char* buffer, const char* tail);
	static const char*			skipSeparator(const char* buffer, const char* tail);
	//static void				shutdown(void);

	//virtual StorageTable* getStorageTable(const char* name, int impureSize, bool tempTable);
	virtual StorageTable* getStorageTable(StorageTableShare* share);
	virtual void	close(void);
	virtual int		commit(void);
	virtual int		prepare(int xidLength, const unsigned char *xid);
	virtual int		rollback(void);
	virtual int		startTransaction(int isolationLevel);
	virtual int		startImplicitTransaction(int isolationLevel);
	virtual int 	endImplicitTransaction(void);
	virtual int		savepointSet();
	virtual int		savepointRelease(int savePoint);
	virtual int		savepointRollback(int savePoint);
	virtual int		markVerb();
	virtual int	    rollbackVerb();
	virtual void	releaseVerb();
	virtual void	addRef(void);
	virtual void	release(void);
	virtual void	dropDatabase(void);
	virtual void	expunge(void);
	virtual void	validate(void);
	virtual const char* getLastErrorString(void);
	virtual int		getMaxKeyLength(void);
	virtual void	 validate(int options);
	
	void			setErrorText(const char* text);
	int				setErrorText(SQLException *exception);
	void			connect(void);
	void			create(void);
	bool			matches(const char* pathname);
	void			remove(StorageTable* storageTable);
	void			databaseDropped(StorageDatabase *database);
	void			disconnect(void);
	void			setMySqlThread(THD* thd);
	void			setCurrentStatement(const char* text);
	Transaction*	getTransaction(void);
	int				translateError(SQLException& exception, int defaultStorageError);
	
	Connection		*connection;
	Database		*database;
	StorageConnection	*collision;
	StorageDatabase	*storageDatabase;
	StorageHandler	*storageHandler;
	THD				*mySqlThread;
	JString			name;
	JString			filename;
	JString			path;
	JString			lastErrorText;
	Stream			*traceStream;
	int				transactionActive;
	int				useCount;
	int				implicitTransactionCount;
    int				verbMark;
    int				mySqlThreadId;
    bool			prepared;
};

#endif
