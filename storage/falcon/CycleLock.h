#ifndef _CYCLE_LOCK_H_
#define _CYCLE_LOCK_H_

class SyncObject;
class CycleManager;
class Database;
class Thread;

class CycleLock
{
public:
	CycleLock(Database *database);
	~CycleLock(void);
	
	static CycleLock*	unlock(void);
	static bool			isLocked(void);

	void				lockCycle(void);
	void				unlockCycle(void);

	SyncObject			*syncObject;
	CycleManager		*cycleManager;
	CycleLock			*chain;
	Thread				*thread;
	bool				locked;
};

#endif
