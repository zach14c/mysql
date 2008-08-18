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
	numberIndexes = 0;

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
		
	for (uint n = 0; n < indexes.length; n++)
		if (indexes.vector[n])
			delete indexes.get(n);
}

void StorageTableShare::lock(bool exclusiveLock)
{
	//syncObject->lock(NULL, (exclusiveLock) ? Exclusive : Shared);
	syncIndexes->lock(NULL, (exclusiveLock) ? Exclusive : Shared);
}

void StorageTableShare::unlock(void)
{
	//syncObject->unlock();
	syncIndexes->unlock();
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

int StorageTableShare::createIndex(StorageConnection *storageConnection, StorageIndexDesc *indexDesc, int indexCount, const char *sql)
{
	if (!table)
		open();

	// Always get syncIndexes before syncObject
	
	Sync syncIndex(syncIndexes, "StorageTableShare::createIndex(1)");
	syncIndex.lock(Exclusive);
	
	Sync syncObj(syncObject, "StorageTableShare::createIndex(2)");
	syncObj.lock(Exclusive);
	
	int ret = storageDatabase->createIndex(storageConnection, table, sql);
	
	if (!ret)
		ret = setIndex(indexCount, indexDesc);
		
	return ret;
}

int StorageTableShare::dropIndex(StorageConnection *storageConnection, StorageIndexDesc *indexDesc, const char *sql)
{
	if (!table)
		open();

	// Always get syncIndexes before syncObject

	Sync syncIndex(syncIndexes, "StorageTableShare::dropIndex(1)");
	syncIndex.lock(Exclusive);
	
	Sync syncObj(syncObject, "StorageTableShare::dropIndex(2)");
	syncObj.lock(Exclusive);
	
	int ret = storageDatabase->dropIndex(storageConnection, table, sql);
	
	if (!ret)
		clearIndex(indexDesc);
				
	return ret;
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

void StorageTableShare::resizeIndexes(int indexCount)
{
	if (indexCount <= 0)
		return;
	
	if ((uint)indexCount > indexes.length)
		indexes.extend(indexCount + 5);

	numberIndexes = indexCount;
}

int StorageTableShare::setIndex(int indexCount, const StorageIndexDesc *indexInfo)
		{
	int indexId = indexInfo->id;
		
	if ((uint)indexId >= indexes.length || numberIndexes < indexCount)
		resizeIndexes(indexCount);
		
	// Allocate a new index if necessary
	
	StorageIndexDesc *indexDesc = indexes.get(indexId);
	
	if (!indexDesc)
		indexes.vector[indexId] = indexDesc = new StorageIndexDesc(indexId);
	
	// Copy index description info
	
	*indexDesc = *indexInfo;

	// Find the corresponding Falcon index
	
	if (indexDesc->primaryKey)
		indexDesc->index = table->primaryKey;
	else
		{
		char indexName[indexNameSize];
		sprintf(indexName, "%s$%s", name.getString(), indexDesc->name.getString());
		indexDesc->index = table->findIndex(indexName);
		}

	int ret = 0;
	
	if (indexDesc->index)
		indexDesc->segmentRecordCounts = indexDesc->index->recordsPerSegment;
	else
		ret = StorageErrorNoIndex;
	
	ASSERT((!ret ? validateIndexes() : true));
		
	return ret;
}

void StorageTableShare::clearIndex(StorageIndexDesc *indexDesc)
{
	if (numberIndexes > 0)
		{
		for (int n = indexDesc->id; n < numberIndexes-1; n++)
			{
			indexes.vector[n] = indexes.vector[n+1];
			indexes.vector[n]->id = n; // assume that index id will match server
			}
			
		indexes.zap(numberIndexes-1);
		numberIndexes--;
		}
		
	ASSERT(validateIndexes());
}

bool StorageTableShare::validateIndexes()
{
	for (int n = 0; n < numberIndexes; n++)
		{
		StorageIndexDesc *indexDesc = indexes.get(n);
		if (indexDesc && indexDesc->id != n)
			return false;
		}
			
	return true;
}

StorageIndexDesc* StorageTableShare::getIndex(int indexId)
{
	if (!indexes.length || indexId >= numberIndexes)
		return NULL;
	
	ASSERT(validateIndexes());
	
	return indexes.get(indexId);
}

StorageIndexDesc* StorageTableShare::getIndex(int indexId, StorageIndexDesc *indexDesc)
{
	StorageIndexDesc *index;
	
	if (!indexes.length || indexId >= numberIndexes)
		index = NULL;
	else
		{
		Sync sync(syncObject, "StorageTableShare::getIndex");
		sync.lock(Shared);
	
		ASSERT(validateIndexes());

		index = indexes.get(indexId);
	
		if (index)
			*indexDesc = *index;
		}
		
	return index;
}

StorageIndexDesc* StorageTableShare::getIndex(const char *name)
{
	Sync sync(syncObject, "StorageTableShare::getIndex(name)");
	sync.lock(Shared);
	
	for (int i = 0; i < numberIndexes; i++)
		{
		StorageIndexDesc *indexDesc = indexes.get(i);
		if (indexDesc && indexDesc->name == name)
			return indexDesc;
		}

	return NULL;
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

// Get index id using the internal (Falcon) index name

int StorageTableShare::getIndexId(const char* schemaName, const char* indexName)
{
	if (indexes.length > 0)
		for (int n = 0; n < numberIndexes; ++n)
			{
			Index *index = indexes.get(n)->index;
			
			if (strcmp(index->getIndexName(), indexName) == 0 &&
				strcmp(index->getSchemaName(), schemaName) == 0)
				{
//				if (n != indexes.get(n)->id) //debug
//					return n;
				return n;
			}
			}
		
	return -1;
}

int StorageTableShare::haveIndexes(int indexCount)
{
	if (indexes.length == 0)
		return false;
		
	if (indexCount > numberIndexes)
		return false;
	
	for (int n = 0; n < numberIndexes; ++n)
		{
		StorageIndexDesc* index = indexes.get(n);
		if (!index)
			return false;
			
		if (index && !index->index)
			return false;
		}
	
	return true;
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
