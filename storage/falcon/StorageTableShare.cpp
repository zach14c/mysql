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

#include <memory.h>
#include <string.h>
#include <stdio.h>
#include "Engine.h"
#include "StorageTableShare.h"
#include "StorageDatabase.h"
#include "StorageHandler.h"
#include "SyncObject.h"
#include "Sync.h"
#include "Sequence.h"
#include "Index.h"
#include "Table.h"
#include "Field.h"
#include "Interlock.h"
#include "CollationManager.h"
#include "MySQLCollation.h"
#include "Connection.h"
#include "PreparedStatement.h"
#include "ResultSet.h"
#include "SQLException.h"
#include "CmdGen.h"

static const char *FALCON_TEMPORARY		= "/falcon_temporary";
static const char *DB_ROOT				= ".fts";

#if defined(_WIN32) && MYSQL_VERSION_ID < 0x50100
#define IS_SLASH(c)	(c == '/' || c == '\\')
#else
#define IS_SLASH(c)	(c == '/')
#endif

#ifdef _DEBUG
#undef THIS_FILE
static const char THIS_FILE[]=__FILE__;
#endif

StorageIndexDesc::StorageIndexDesc()
{
	id = 0;
	unique = 0;
	primaryKey = 0;
	numberSegments = 0;
	index = NULL;
	segmentRecordCounts = NULL;
	next = NULL;
	name[0] = '\0';
	rawName[0] = '\0';
};

StorageIndexDesc::StorageIndexDesc(const StorageIndexDesc *indexInfo)
{
	if (indexInfo)
		*this = *indexInfo;
	else
		{
		id = 0;
		unique = 0;
		primaryKey = 0;
		numberSegments = 0;
		segmentRecordCounts = NULL;
		name[0] = '\0';
		rawName[0] = '\0';
		}
		
	index = NULL;
	next = NULL;
	prev = NULL;
};

StorageIndexDesc::~StorageIndexDesc(void)
{
}

//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////

StorageTableShare::StorageTableShare(StorageHandler *handler, const char * path, const char *tableSpaceName, int lockSize, bool tempTbl)
{
	storageHandler = handler;
	storageDatabase = NULL;
	impure = new UCHAR[lockSize];
	initialized = false;
	table = NULL;
	format = NULL;
	syncObject = new SyncObject;
	syncObject->setName("StorageTableShare::syncObject");
	syncIndexes = new SyncObject;
	syncIndexes->setName("StorageTableShare::syncIndexes");
	sequence = NULL;
	tempTable = tempTbl;
	setPath(path);
	indexes = NULL;

	if (tempTable)
		tableSpace = TEMPORARY_TABLESPACE;
	else if (tableSpaceName && tableSpaceName[0])
		tableSpace = JString::upcase(tableSpaceName);
	else
		tableSpace = schemaName;
}

StorageTableShare::~StorageTableShare(void)
{
	delete syncObject;
	delete syncIndexes;
	delete [] impure;
	
	if (storageDatabase)
		storageDatabase->release();
		
	for (StorageIndexDesc *indexDesc; (indexDesc = indexes);)
		{
		indexes = indexDesc->next;
		delete indexDesc;
		}
}

void StorageTableShare::lock(bool exclusiveLock)
{
	syncObject->lock(NULL, (exclusiveLock) ? Exclusive : Shared);
}

void StorageTableShare::unlock(void)
{
	syncObject->unlock();
}

void StorageTableShare::lockIndexes(bool exclusiveLock)
{
	syncIndexes->lock(NULL, (exclusiveLock) ? Exclusive : Shared);
}

void StorageTableShare::unlockIndexes(void)
{
	syncIndexes->unlock();
}

int StorageTableShare::open(void)
{
	if (!table)
		{
		table = storageDatabase->findTable(name, schemaName);
		
		if (table)
			format = table->getCurrentFormat();
			
		sequence = storageDatabase->findSequence(name, schemaName);
		}
	
	if (!table)
		return StorageErrorTableNotFound;
	
	return 0;
}

int StorageTableShare::create(StorageConnection *storageConnection, const char* sql, int64 autoIncrementValue)
{
	try
		{
		table = storageDatabase->createTable(storageConnection, name, schemaName, sql, autoIncrementValue);
		}
	catch (SQLException& exception)
		{
		int sqlcode= exception.getSqlcode();
		switch (sqlcode)
			{
			case TABLESPACE_NOT_EXIST_ERROR:
				return StorageErrorTableSpaceNotExist;
			default:
				return StorageErrorTableExits;
			}
		}
	if (!table)
		return StorageErrorTableExits;
	
	format = table->getCurrentFormat();

	if (autoIncrementValue)
		sequence = storageDatabase->findSequence(name, schemaName);
		
	return 0;
}

int StorageTableShare::upgrade(StorageConnection *storageConnection, const char* sql, int64 autoIncrementValue)
{
	if (!(table = storageDatabase->upgradeTable(storageConnection, name, schemaName, sql, autoIncrementValue)))
		return StorageErrorTableExits;

	format = table->getCurrentFormat();
	
	if (autoIncrementValue)
		sequence = storageDatabase->findSequence(name, schemaName);
		
	return 0;
}

int StorageTableShare::deleteTable(StorageConnection *storageConnection)
{
	int res = storageDatabase->deleteTable(storageConnection, this);
	
	if (res == 0 || res == StorageErrorTableNotFound)
		{
		unRegisterTable();
		
		storageHandler->removeTable(this);
			
		delete this;
		}

	return res;
}

int StorageTableShare::truncateTable(StorageConnection *storageConnection)
{
	int res = storageDatabase->truncateTable(storageConnection, this);
	
	return res;
}

const char* StorageTableShare::cleanupFieldName(const char* name, char* buffer, int bufferLength)
{
	char *q = buffer;
	char *end = buffer + bufferLength - 1;
	const char *p = name;
	bool quotes = false;
	
	for (; *p && q < end; ++p)
		{
		if (*p == '"')
			{
			*q++ = UPPER(*p);
			quotes = !quotes;
			}
	
		if (quotes)
			*q++ = *p;
		else
			*q++ = UPPER(*p);
		}

	*q = 0;
	
	return buffer;
}

const char* StorageTableShare::cleanupTableName(const char* name, char* buffer, int bufferLength, char *schema, int schemaLength)
{
	char *q = buffer;
	const char *p = name;
	
	while (*p == '.')
		++p;

	for (; *p; ++p)
		if (*p == '/' || *p == '\\')
			{
			*q = 0;
			strcpy(schema, buffer);
			q = buffer;
			}
		else
			*q++ = *p; //UPPER(*p);
	
	*q = 0;
	
	if ( (q = strchr(buffer, '.')) )
		*q = 0;
	
	return buffer;
}

char* StorageTableShare::createIndexName(const char *rawName, char *indexName)
{
	char nameBuffer[indexNameSize];
	cleanupFieldName(rawName, nameBuffer, sizeof(nameBuffer));
	sprintf(indexName, "%s$%s", name.getString(), nameBuffer);
	return indexName;
}

int StorageTableShare::createIndex(StorageConnection *storageConnection, StorageIndexDesc *indexDesc, const char *sql)
{
	if (!table)
		open();

	// Lock out other clients before locking the table
	
	Sync syncIndex(syncIndexes, "StorageTableShare::createIndex(1)");
	syncIndex.lock(Exclusive);
	
	Sync syncObj(syncObject, "StorageTableShare::createIndex(2)");
	syncObj.lock(Exclusive);
	
	int ret = storageDatabase->createIndex(storageConnection, table, sql);
	
	if (!ret)
		ret = setIndex(indexDesc);
		
	return ret;
}

void StorageTableShare::addIndex(StorageIndexDesc *indexDesc)
{
	if (!getIndex(indexDesc->id))
		{
		if (indexes)
			{
			indexDesc->next = indexes;
			indexDesc->prev = NULL;
			indexes->prev = indexDesc;
			}
		
		indexes = indexDesc;
		}
}

void StorageTableShare::deleteIndex(int indexId)
{
	for (StorageIndexDesc *indexDesc = indexes; indexDesc; indexDesc = indexDesc->next)
		if (indexDesc->id == indexId)
			{
			if (indexDesc->prev)
				indexDesc->prev->next = indexDesc->next;
			else
				indexes = indexDesc->next;
				
			if (indexDesc->next)
				indexDesc->next->prev = indexDesc->prev;
				
			delete indexDesc;	
			break;
			}
}

int StorageTableShare::dropIndex(StorageConnection *storageConnection, StorageIndexDesc *indexDesc, const char *sql)
{
	if (!table)
		open();

	// Lock out other clients before locking the table

	Sync syncIndex(syncIndexes, "StorageTableShare::dropIndex(1)");
	syncIndex.lock(Exclusive);
	
	Sync syncObj(syncObject, "StorageTableShare::dropIndex(2)");
	syncObj.lock(Exclusive);
	
	int ret = storageDatabase->dropIndex(storageConnection, table, sql);
	
	if (!ret)
		deleteIndex(indexDesc->id);
				
	return ret;
}

void StorageTableShare::deleteIndexes()
{
	for (StorageIndexDesc *indexDesc; (indexDesc = indexes);)
		{
		indexes = indexDesc->next;
		delete indexDesc;
		}
}

int StorageTableShare::renameTable(StorageConnection *storageConnection, const char* newName)
{
	char tableName[256];
	char schemaName[256];
	cleanupTableName(newName, tableName, sizeof(tableName), schemaName, sizeof(schemaName));
	int ret = storageDatabase->renameTable(storageConnection, table, JString::upcase(tableName), JString::upcase(schemaName));

	if (ret)
		return ret;
	
	unRegisterTable();
	storageHandler->removeTable(this);
	setPath(newName);
	registerTable();
	storageHandler->addTable(this);
	
	return ret;
}

int StorageTableShare::setIndex(const StorageIndexDesc *indexInfo)
{
	int ret = 0;

	if (!getIndex(indexInfo->id))
		{
		StorageIndexDesc *indexDesc = new StorageIndexDesc(indexInfo);
		addIndex(indexDesc);

	// Find the corresponding Falcon index
	
	if (indexDesc->primaryKey)
		indexDesc->index = table->primaryKey;
	else
		{
		char indexName[indexNameSize];
			sprintf(indexName, "%s$%s", name.getString(), indexDesc->name);
		indexDesc->index = table->findIndex(indexName);
		}

	if (indexDesc->index)
		indexDesc->segmentRecordCounts = indexDesc->index->recordsPerSegment;
	else
		ret = StorageErrorNoIndex;
		}
		
	return ret;
}

StorageIndexDesc* StorageTableShare::getIndex(int indexId)
{
	if (!indexes)
		return NULL;
			
	for (StorageIndexDesc *indexDesc = indexes; indexDesc; indexDesc = indexDesc->next)
		if (indexDesc->id == indexId)
			return indexDesc;
		
	return NULL;
}

StorageIndexDesc* StorageTableShare::getIndex(int indexId, StorageIndexDesc *indexDesc)
{
	if (!indexes)
		return NULL;
	
	Sync sync(syncIndexes, "StorageTableShare::getIndex");
	sync.lock(Shared);
	
	StorageIndexDesc *index = getIndex(indexId);
			
	if (index)
		*indexDesc = *index;
		
	return index;
}

StorageIndexDesc* StorageTableShare::getIndex(const char *name)
{
	if (!indexes)
		return NULL;
	
	Sync sync(syncIndexes, "StorageTableShare::getIndex(name)");
	sync.lock(Shared);
	
	for (StorageIndexDesc *indexDesc = indexes; indexDesc; indexDesc = indexDesc->next)
		if (indexDesc->name == name)
			return indexDesc;
			
	return NULL;
}

int StorageTableShare::getIndexId(const char* schemaName, const char* indexName)
{
	if (!indexes)
		return -1;
	
	for (StorageIndexDesc *indexDesc = indexes; indexDesc; indexDesc = indexDesc->next)
		{
		Index *index = indexDesc->index;
	
		if (index)
			if (strcmp(index->getIndexName(), indexName) == 0 &&
				strcmp(index->getSchemaName(), schemaName) == 0)
				return indexDesc->id;
		}
		
	return -1;
}

int StorageTableShare::haveIndexes(int indexCount)
{
	if (!indexes)
		return false;
	
	int n = 0;
	for (StorageIndexDesc *indexDesc = indexes; indexDesc; indexDesc = indexDesc->next, n++)
		{
		if (!indexDesc->index)
			return false;
		}

	return (n == indexCount);
}

INT64 StorageTableShare::getSequenceValue(int delta)
{
	if (!sequence)
		return 0;

	return sequence->update(delta, NULL);
}

int StorageTableShare::setSequenceValue(INT64 value)
{
	if (!sequence)
		return StorageErrorNoSequence;
		
	Sync sync(syncObject, "StorageTableShare::setSequenceValue");
	sync.lock(Exclusive);
	INT64 current = sequence->update(0, NULL);
	
	if (value > current)
		sequence->update(value - current, NULL);

	return 0;
}

void StorageTableShare::setTablePath(const char* path, bool tmp)
{
	if (pathName.IsEmpty())
		pathName = path;
	
	tempTable = tmp;
}

void StorageTableShare::registerCollation(const char* collationName, void* arg)
{
	JString name = JString::upcase(collationName);
	Collation *collation = CollationManager::findCollation(name);
	
	if (collation)
		{
		collation->release();
		
		return;
		}
	
	collation = new MySQLCollation(name, arg);
	CollationManager::addCollation(collation);
}

void StorageTableShare::load(void)
{
	Sync sync(&storageHandler->dictionarySyncObject, "StorageTableShare::load");
	sync.lock(Exclusive);
	Connection *connection = storageHandler->getDictionaryConnection();
	if (!connection)
		return;
	PreparedStatement *statement = connection->prepareStatement(
		"select given_schema_name,given_table_name,effective_schema_name,effective_table_name,tablespace_name "
		"from falcon.tables where pathname=?");
	statement->setString(1, pathName);
	ResultSet *resultSet = statement->executeQuery();
	
	if (resultSet->next())
		{
		int n = 1;
		givenSchema = resultSet->getString(n++);
		givenName = resultSet->getString(n++);
		schemaName = resultSet->getString(n++);
		name = resultSet->getString(n++);
		tableSpace = resultSet->getString(n++);
		}
	
	resultSet->close();
	statement->close();	
	connection->commit();
}

void StorageTableShare::registerTable(void)
{
	Connection *connection = NULL;
	PreparedStatement *statement = NULL;
	
	try
		{
		Sync sync(&storageHandler->dictionarySyncObject, "StorageTableShare::registerTable");
		sync.lock(Exclusive);
		connection = storageHandler->getDictionaryConnection();
		statement = connection->prepareStatement(
			"replace falcon.tables "
			"(given_schema_name,given_table_name,effective_schema_name,effective_table_name,tablespace_name, pathname)"
			" values (?,?,?,?,?,?)");
		int n = 1;
		statement->setString(n++, givenSchema);
		statement->setString(n++, givenName);
		statement->setString(n++, schemaName);
		statement->setString(n++, name);
		statement->setString(n++, tableSpace);
		statement->setString(n++, pathName);
		statement->executeUpdate();	
		statement->close();	
		connection->commit();
		}
	catch (SQLException&)
		{
		if (statement)
			statement->close();
			
		if (connection)
			connection->commit();
		}
}


void StorageTableShare::unRegisterTable(void)
{
	Sync sync(&storageHandler->dictionarySyncObject, "StorageTableShare::unRegisterTable");
	sync.lock(Exclusive);
	Connection *connection = storageHandler->getDictionaryConnection();
	PreparedStatement *statement = connection->prepareStatement(
		"delete from falcon.tables where pathname=?");
	statement->setString(1, pathName);
	statement->executeUpdate();	
	statement->close();	
	connection->commit();
}

void StorageTableShare::getDefaultPath(char *buffer)
{
	const char *slash = NULL;
	const char *p;

	for (p = pathName; *p; p++)
		if (IS_SLASH(*p))
			slash = p;

	if (!slash)
		slash = p;

	IPTR len = slash - pathName + 1;
	
	if (tempTable)
		len += sizeof(FALCON_TEMPORARY);
		
	char *q = buffer;

	for (p = pathName; p < slash; )
		{
		char c = *p++;
		*q++ = (IS_SLASH(c)) ? '/' : c;
		}

	if (tempTable)
		for (p = FALCON_TEMPORARY; *p;)
			*q++ = *p++;
			
	strcpy(q, DB_ROOT);
}

void StorageTableShare::setPath(const char* path)
{
	pathName = path;
	char tableName[256];
	char schema[256];
	cleanupTableName(path, tableName, sizeof(tableName), schema, sizeof(schema));
	givenName = tableName;
	givenSchema = schema;
	tableSpace = JString::upcase(givenSchema);
	name = JString::upcase(tableName);
	schemaName = JString::upcase(schema);
}

void StorageTableShare::findDatabase(void)
{
	load();
	const char *dbName = (tableSpace.IsEmpty()) ? MASTER_NAME : tableSpace;
	storageDatabase = storageHandler->findDatabase(dbName);
}

const char* StorageTableShare::getDefaultRoot(void)
{
	return DB_ROOT;
}

void StorageTableShare::setDatabase(StorageDatabase* db)
{
	if (storageDatabase)
		storageDatabase->release();
	
	if ( (storageDatabase = db) )
		storageDatabase->addRef();
}

uint64 StorageTableShare::estimateCardinality(void)
{
	return table->estimateCardinality();
}

bool StorageTableShare::tableExists(void)
{
	JString path = lookupPathName();
	
	return !path.IsEmpty();
}

JString StorageTableShare::lookupPathName(void)
{
	Sync sync(&storageHandler->dictionarySyncObject, "StorageTableShare::lookupPathName");
	sync.lock(Exclusive);
	Connection *connection = storageHandler->getDictionaryConnection();
	PreparedStatement *statement = connection->prepareStatement(
		"select pathname from falcon.tables where effective_schema_name=? and effective_table_name=?");
	int n = 1;
	statement->setString(n++, schemaName);
	statement->setString(n++, name);
	ResultSet *resultSet = statement->executeQuery();
	JString path;
		
	if (resultSet->next())
		path = resultSet->getString(1);
	
	statement->close();
	connection->commit();
	
	return path;
}


int StorageTableShare::getFieldId(const char* fieldName)
{
	Field *field = table->findField(fieldName);
	
	if (!field)
		return -1;
	
	return field->id;
}
