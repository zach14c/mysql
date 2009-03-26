#ifndef _CYCLE_MANAGER_H_
#define _CYCLE_MANAGER_H_

#include "SyncObject.h"

class Thread;
class Database;
class Record;
class RecordVersion;

class CycleManager
{
	struct RecordList
		{
		Record		*record;
		RecordList	*next;
		};
		
public:
	CycleManager(Database *database);
	~CycleManager(void);
	
	void		start(void);
	void		shutdown(void);
	void		cycleManager(void);
	void		queueForDelete(Record* record);

	static void cycleManager(void *arg);
	
	SyncObject		cycle1;
	SyncObject		cycle2;
	SyncObject		*currentCycle;
	RecordVersion	*recordVersions;
	RecordList		*records;
	Thread			*thread;
	Database		*database;
};

#endif
