#ifndef _CYCLE_MANAGER_H_
#define _CYCLE_MANAGER_H_

#include "SyncObject.h"

class Thread;
class Database;
class Record;
class RecordVersion;
class Value;

static const int syncArraySize = 64;
static const int syncArrayMask = 63;

class CycleManager
{
	struct RecordList
		{
		Record		*zombie;
		RecordList	*next;
		};

	struct ValueList
		{
		Value		**zombie;
		ValueList	*next;
		};

	struct BufferList
		{
		char		*zombie;
		BufferList	*next;
		};
		
public:
	CycleManager(Database *database);
	~CycleManager(void);
	
	void		start(void);
	void		shutdown(void);
	void		cycleManager(void);
	SyncObject *getSyncObject(void);
	void		queueForDelete(Record* zombie);
	void		queueForDelete(Value** zombie);
	void		queueForDelete(char* zombie);

	static void cycleManager(void *arg);
	
	SyncObject		**cycle1;
	SyncObject		**cycle2;
	SyncObject		**currentCycle;
	RecordVersion	*recordVersionPurgatory;
	RecordList		*recordPurgatory;
	ValueList		*valuePurgatory;
	BufferList		*bufferPurgatory;
	Thread			*thread;
	Database		*database;
};

#endif
