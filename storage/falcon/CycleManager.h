#ifndef _CYCLE_MANAGER_H_
#define _CYCLE_MANAGER_H_

#include "SyncObject.h"

class Thread;
class Database;
class RecordVersion;

class CycleManager
{
public:
	CycleManager(Database *database);
	~CycleManager(void);
	
	void		start(void);
	void		shutdown(void);
	void		cycleManager(void);
	void		queueForDelete(RecordVersion* record);

	static void cycleManager(void *arg);
	
	SyncObject		cycle1;
	SyncObject		cycle2;
	SyncObject		*currentCycle;
	RecordVersion	*records;
	Thread			*thread;
	Database		*database;
};

#endif
