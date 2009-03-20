/* Copyright (C) 2006-2008 MySQL AB, 2008-2009 Sun Microsystems, Inc.

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

// Database.h: interface for the Database class.
//
//////////////////////////////////////////////////////////////////////

#if !defined(AFX_DATABASE_H__5EC961D1_A406_11D2_AB5B_0000C01D2301__INCLUDED_)
#define AFX_DATABASE_H__5EC961D1_A406_11D2_AB5B_0000C01D2301__INCLUDED_

#if _MSC_VER >= 1000
#pragma once
#endif // _MSC_VER >= 1000

#include "SyncObject.h"

#define ODS_VERSION1		1
#define ODS_VERSION2		2		// Fixed numeric index sign problem, 1/22/2002
#define ODS_VERSION			ODS_VERSION2

#define ODS_MINOR_VERSION0	0
#define ODS_MINOR_VERSION1	1		// Has serial log
#define ODS_MINOR_VERSION2	2		// Has SequencePages external to the section tree
#define ODS_MINOR_VERSION3	3		// Switch to variable length record numbers in index
#define ODS_MINOR_VERSION	ODS_MINOR_VERSION3

#define COMBINED_VERSION(major,minor)	(major * 100 + minor)
#define VERSION_CURRENT					COMBINED_VERSION(ODS_VERSION, ODS_MINOR_VERSION)					
#define VERSION_SERIAL_LOG				COMBINED_VERSION(ODS_VERSION2, ODS_MINOR_VERSION1)

// Reserved tablespaces

static const int TABLESPACE_ID_BACKLOG		= -1;
static const int TABLESPACE_ID_RESERVED		= -2;

static const int FALC0N_TRACE_TRANSACTIONS	= 1;
static const int FALC0N_SYNC_TEST			= 2;
static const int FALC0N_SYNC_OBJECTS		= 4;
static const int FALC0N_FREEZE				= 8;
static const int FALC0N_REPORT_WRITES		= 16;
static const int FALC0N_SYNC_HANDLER		= 32;
static const int FALC0N_TEST_BITMAP			= 64;

// This constant defines how many times a thread will sleep(10) while 
// waiting for record cache memory to be freed up by the scavenger thread
// before it gives up and returns OUT_OF_RECORD_MEMORY_ERROR.

static const int OUT_OF_RECORD_MEMORY_RETRIES = 10;

// Milliseconds per iteration to wait for the Scavenger

static const int SCAVENGE_WAIT_MS			  = 50;

// Scavenger cycles per call to updateCardinalities()

static const int CARDINALITY_FREQUENCY		  = 20;


#define TABLE_HASH_SIZE		101

class Table;
class UnTable;
class Dbb;
CLASS(Statement);
class PreparedStatement;
class CompiledStatement;
class Table;
class Connection;
class Transaction;
class TransactionManager;
class Stream;
class InversionFilter;
class Bitmap;
class ResultList;
class ResultSet;
class TemplateManager;
class TemplateContext;
class Inversion;
class Java;
class Application;
class Applications;
class Threads;
class Scheduler;
class Scavenger;
class Session;
class SessionManager;
class RoleModel;
class ImageManager;
class LicenseManager;
class Sequence;
class SequenceManager;
class Repository;
class RepositoryManager;
class User;
class Role;
class SymbolManager;
class FilterSetManager;
class TableSpaceManager;
class Coterie;
class Parameters;
class SearchWords;
class Cache; 
class Schema;
class Configuration;
class SerialLog;
class PageWriter;
class IndexKey;
class InfoTable;
class TableSpace;
class MemMgr;
class MemControl;
class RecordScavenge;
class PriorityScheduler;
class SQLException;
class BackLog;
class SyncHandler;
class CycleManager;

struct JavaCallback;

class Database
{
public:
	Database(const char *dbName, Configuration *config, Threads *parent);
	virtual ~Database();

	void			shutdownNow();
	void			dropDatabase();
	void			start();
	void			deleteRepositoryBlob(const char *schema, const char *repositoryName, int volume, int64 blobId, Transaction *transaction);
	void			deleteRepository (Repository *repository);
	Schema*			getSchema (const char *schemaName);
	Repository*		createRepository(const char *name, const char *schema, Sequence *sequence, const char *fileName, int volume, const char *rolloverString);
	Repository*		getRepository(const char *schema, const char *name);
	Repository*		findRepository(const char *schema, const char *name);
	const char*		fetchTemplate (JString applicationName, JString templateName, TemplateContext *context);
	void			licenseCheck();
	void			serverOperation (int op, Parameters *parameters);
	void			scavenge(bool forced = false);
	void			scavengeCompiledStatements(void);
	void			scavengeRecords(bool forced = false);
	void			pruneRecords(RecordScavenge* recordScavenge);
	void			retireRecords(RecordScavenge* recordScavenge);
	int				getMemorySize (const char *string);
	JString			analyze(int mask);
	void			upgradeSystemTables();
	const char*		getString (const char *string);
	const char*		getSymbol (const WCString *string);
	bool			isSymbol (const char *string);
	const char*		getSymbol (const char *string);
	Role*			findRole (const WCString *schema, const WCString *roleName);
	PreparedStatement* prepareStatement (Connection *connection, const WCString *sqlStr);
	CompiledStatement* compileStatement (Connection *connection, JString sqlString);
	CompiledStatement* getCompiledStatement (Connection *connection, const WCString *sqlString);
	void			rebuildIndexes();
	void			removeFromInversion (InversionFilter *filter, Transaction *transaction);
	Transaction*	getSystemTransaction();
	int64			updateSequence (int sequenceId, int64 delta, Transaction *transaction);
	int				createSequence(int64 initialValue);
	void			ticker();
	static void		ticker (void *database);
	static void		cardinalityThreadMain(void * database);
	void			cardinalityThreadMain(void);
	void			cardinalityThreadWakeup(void);
	static void		scavengerThreadMain(void * database);
	void			scavengerThreadMain(void);
	void			scavengerThreadWakeup(void);
	void			validate (int optionMask);
	Role*			findRole(const char *schemaName, const char * roleName);
	User*			findUser (const char *account);
	User*			createUser (const char *account, const char *password, bool encrypted, Coterie *coterie);
	int				getMaxKeyLength(void);

#ifndef STORAGE_ENGINE
	void		startSessionManager();
	void		genHTML (ResultSet *resultSet, const char *series, const char *type, 
						 TemplateContext *context, Stream *stream, JavaCallback *callback);
	void		genHTML(ResultSet * resultSet, const WCString *series, const WCString *type, TemplateContext *context, Stream *stream, JavaCallback *callback);
	JString		expandHTML(ResultSet *resultSet, const WCString *applicationName, const char *source, TemplateContext *context, JavaCallback *callback);
	void		zapLinkages();
	void		checkManifest();
	void		detachDebugger();
	JString		debugRequest(const char *request);
	int			attachDebugger();
	Application* getApplication (const char *applicationName);
#endif

	void			invalidateCompiledStatements (Table *table);
	void			shutdown();
	void			execute (const char *sql);
	void			addTable (Table *table);
	void			truncateTable(Table *table, Sequence *sequence, Transaction *transaction);
	void			dropTable (Table *table, Transaction *transaction);
	void			flushInversion(Transaction *transaction);
	bool			matches (const char *fileName);
	Table*			loadTable (ResultSet *resultSet);
	Table*			getTable (int tableId);
	void			reindex(Transaction *transaction);
	void			search (ResultList *resultList, const char *string);
	int32			addInversion (InversionFilter *filter, Transaction *transaction);
	void			clearDebug();
	void			setDebug();
	void			commitSystemTransaction();
	void			rollbackSystemTransaction(void);
	bool			flush(int64 arg);
	
	Transaction*	startTransaction(Connection *connection);
	CompiledStatement* getCompiledStatement (Connection *connection, const char *sqlString);
	void			openDatabase (const char *filename);
	PreparedStatement* prepareStatement (Connection* connection, const char *sqlStr);
	Statement*		createStatement (Connection *connection);
	void			release();
	void			addRef();
	PreparedStatement* prepareStatement (const char *sqlStr);
	void			addSystemTables();
	Table*			addTable (User *owner, const char *name, const char *schema, TableSpace *tableSpace);
	Table*			findTable (const char *schema, const char *name);
	Statement*		createStatement();
	virtual void	createDatabase (const char *filename);
	void			renameTable(Table* table, const char* newSchema, const char* newName);
	bool			hasUncommittedRecords(Table* table, Transaction *transaction);
	void			waitForWriteComplete(Table *table);
	void			validateCache(void);
	int				recoverGetNextLimbo(int xidSize, unsigned char *xid);
	void			commitByXid(int xidLength, const UCHAR* xid);
	void			rollbackByXid(int xidLength, const UCHAR* xid);
	void			getTransactionSummaryInfo(InfoTable* infoTable);
	void			getTableSpaceInfo(InfoTable* infoTable);
	void			getTableSpaceFilesInfo(InfoTable* infoTable);
	void			updateCardinalities(void);
	void			getIOInfo(InfoTable* infoTable);
	void			getTransactionInfo(InfoTable* infoTable);
	void			getSerialLogInfo(InfoTable* infoTable);
	void			sync();
	void			preUpdate();
	void			setRecordMemoryMax(uint64 value);
	void			setRecordScavengeThreshold(int value);
	void			setRecordScavengeFloor(int value);
	void			checkRecordScavenge(void);
	void			signalCardinality(void);
	void			signalScavenger(bool force = false);
	void			debugTrace(void);
	void			pageCacheFlushed(int64 flushArg);
	JString			setLogRoot(const char *defaultPath, bool create);
	void			setIOError(SQLException* exception);
	void			clearIOError(void);
	void			flushWait(void);
	void			setLowMemory(uint64 spaceNeeded);
	void			clearLowMemory(void);


	Dbb					*dbb;
	Cache				*cache;
	JString				name;
	JString				ioError;
	Database			*next;					// used by Connection
	Database			*prior;					// used by Connection
	Schema				*schemas [TABLE_HASH_SIZE];
	Table				*tables [TABLE_HASH_SIZE];
	Table				*tablesModId [TABLE_HASH_SIZE];
	Table				*tableList;
	Table				*zombieTables;
	UnTable				*unTables [TABLE_HASH_SIZE];
	CompiledStatement	*compiledStatements;
	Configuration		*configuration;
	SerialLog			*serialLog;
	Connection			*systemConnection;
	BackLog				*backLog;
	int					nextTableId;
	bool				formatting;
	bool				licensed;
	bool				fieldExtensions;
	bool				utf8;
	bool				panicShutdown;
	bool				shuttingDown;
	bool				longSync;
	bool				lowMemory;
	int					useCount;
	int					sequence;
	int					stepNumber;
	int					scavengeCycle;
	Java				*java;
	Applications		*applications;
	SyncObject			syncObject;
	SyncObject			syncTables;
	SyncObject			syncStatements;   // exclusive lock ok only if syncTables not exclusive
	SyncObject			syncAddStatement;
	SyncObject			syncResultSets;
	SyncObject			syncConnectionStatements;
	SyncObject			syncScavenge;
	SyncObject			syncSysDDL;
	Mutex				syncCardinality;
	Mutex				syncMemory;
	PriorityScheduler	*ioScheduler;
	Threads				*threads;
	Scheduler			*scheduler;
	Scheduler			*internalScheduler;
	Scavenger			*scavenger;
#ifndef STORAGE_ENGINE
	Scavenger			*garbageCollector;
#endif
	TemplateManager		*templateManager;
	ImageManager		*imageManager;
	SessionManager		*sessionManager;
	RoleModel			*roleModel;
	LicenseManager		*licenseManager;
	SequenceManager		*sequenceManager;
	SymbolManager		*symbolManager;
	RepositoryManager	*repositoryManager;
	TransactionManager	*transactionManager;
	FilterSetManager	*filterSetManager;
	TableSpaceManager	*tableSpaceManager;
	CycleManager		*cycleManager;
	SyncHandler			*syncHandler;
	SearchWords			*searchWords;
	Thread				*tickerThread;
	Thread				*cardinalityThread;
	Thread				*scavengerThread;
	volatile INTERLOCK_TYPE	cardinalityThreadSleeping;
	volatile INTERLOCK_TYPE	cardinalityThreadSignaled;
	volatile INTERLOCK_TYPE	scavengerThreadSleeping;
	volatile INTERLOCK_TYPE	scavengerThreadSignaled;
	volatile INTERLOCK_TYPE	scavengeForced;
	PageWriter			*pageWriter;
	PreparedStatement	*updateCardinality;
	MemControl			*recordMemoryControl;
	MemMgr				*recordDataPool;	// Record data pool (no object metadata)
	MemMgr				*recordPool;		// Record object pool
	MemMgr				*recordVersionPool;	// RecordVersion object pool
	time_t				startTime;
	
	volatile int		deltaTime;
	volatile time_t		timestamp;
	volatile int		numberQueries;
	volatile int		numberRecords;
	volatile int		numberTemplateEvals;
	volatile int		numberTemplateExpands;
	volatile int		pendingIOErrors;
	int					odsVersion;
	int					noSchedule;
	int					pendingIOErrorCode;
	uint32				serialLogBlockSize;

	volatile INTERLOCK_TYPE	currentGeneration;
	uint64				recordMemoryMax;
	uint64				recordScavengeThreshold;
	uint64				recordScavengeMaxGroupSize;
	uint64				recordScavengeFloor;
	uint64				recordPoolAllocCount;
	uint64				lastGenerationMemory;
	uint64				lastActiveMemoryChecked;
	uint64				scavengeCount;
	uint64				lowMemoryCount;
	time_t				creationTime;
	volatile time_t		lastScavenge;
};

#endif // !defined(AFX_DATABASE_H__5EC961D1_A406_11D2_AB5B_0000C01D2301__INCLUDED_)
