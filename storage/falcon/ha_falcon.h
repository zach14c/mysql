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

class StorageConnection;
class StorageTable;
class StorageTableShare;
class CmdGen;
class THD;
class my_decimal;

#define TRUNCATE_ENABLED

static const int TRANSACTION_READ_UNCOMMITTED = 1;	// Dirty reads, non-repeatable reads and phantom reads can occur.
static const int TRANSACTION_READ_COMMITTED   = 2;	// Dirty reads are prevented; non-repeatable reads and phantom reads can occur.
static const int TRANSACTION_WRITE_COMMITTED  = 4;	// Dirty reads are prevented; non-repeatable reads happen after writes; phantom reads can occur.
static const int TRANSACTION_CONSISTENT_READ  = 8;	// Dirty reads and non-repeatable reads are prevented; phantom reads can occur.   
static const int TRANSACTION_SERIALIZABLE     = 16;	// Dirty reads, non-repeatable reads and phantom reads are prevented.

struct TABLE_SHARE;
class StorageIndexDesc;
struct StorageBlob;

class StorageInterface : public handler
{
public:
	StorageInterface(handlerton *, TABLE_SHARE *table_arg);
	~StorageInterface(void);

	virtual int		open(const char *name, int mode, uint test_if_locked);
	virtual const char *table_type(void) const;
	virtual const char **bas_ext(void) const;
	virtual int		close(void);
	virtual ulonglong table_flags(void) const;
	virtual ulong	index_flags(uint idx, uint part, bool all_parts) const;

	virtual int		info(uint what);
	virtual uint	max_supported_keys(void) const;
	virtual uint	max_supported_key_length(void) const;
	virtual uint	max_supported_key_part_length(void) const;

	virtual int		rnd_init(bool scan);
	virtual int		rnd_next(uchar *buf);
	virtual int		rnd_pos(uchar *buf, uchar *pos);
	virtual void	position(const uchar *record);

	virtual int		create(const char *name, TABLE *form, HA_CREATE_INFO *info);
	virtual THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
	                                   enum thr_lock_type lock_type);
	virtual int		delete_table(const char *name);
	virtual int		write_row(uchar *buff);
	virtual int		update_row(const uchar *oldData, uchar *newData);
	virtual int		delete_row(const uchar *buf);
	virtual void	unlock_row(void);

	virtual int		index_read(uchar *buf, const uchar *key, uint keyLen,
					           enum ha_rkey_function find_flag);
	virtual int		index_init(uint idx, bool sorted);
	virtual int		index_end(void);
	virtual int		index_first(uchar* buf);
	virtual int		index_next(uchar *buf);
	virtual int		index_next_same(uchar *buf, const uchar *key, uint key_len);

	virtual ha_rows	records_in_range(uint index,
	                                 key_range *lower, key_range *upper);
	virtual int		rename_table(const char *from, const char *to);
	virtual double	read_time(uint index, uint ranges, ha_rows rows);
	virtual int		read_range_first(const key_range *start_key,
	                                 const key_range *end_key,
	                                 bool eq_range_arg, bool sorted);
	virtual double	scan_time(void);
	virtual int		extra(ha_extra_function operation);
	virtual int		start_stmt(THD *thd, thr_lock_type lock_type);
	virtual int		external_lock(THD* thd, int lock_type);
	virtual const char *index_type(uint key_number);
	virtual void	get_auto_increment(ulonglong offset, ulonglong increment,
	                                  ulonglong nb_desired_values,
	                                  ulonglong *first_value,
	                                  ulonglong *nb_reserved_values);
	virtual bool	get_error_message(int error, String *buf);
	virtual uint8	table_cache_type(void);
	virtual int		add_index(TABLE* table_arg, KEY* key_info, uint num_of_keys);
	virtual bool	check_if_incompatible_data(HA_CREATE_INFO* create_info, uint table_changes);
	virtual void	update_create_info(HA_CREATE_INFO* create_info);
	virtual const COND* cond_push(const COND* cond);
	virtual int		optimize(THD* thd, HA_CHECK_OPT* check_opt);
	virtual int		check(THD* thd, HA_CHECK_OPT* check_opt);
	virtual int		repair(THD* thd, HA_CHECK_OPT* check_opt);
	virtual int		reset();

	virtual int		check_if_supported_alter(TABLE *altered_table, HA_CREATE_INFO *create_info, HA_ALTER_FLAGS *alter_flags, uint table_changes);
	virtual int		alter_table_phase1(THD* thd, TABLE* altered_table, HA_CREATE_INFO* create_info, HA_ALTER_INFO* alter_info, HA_ALTER_FLAGS* alter_flags);
	virtual int		alter_table_phase2(THD* thd, TABLE* altered_table, HA_CREATE_INFO* create_info, HA_ALTER_INFO* alter_info, HA_ALTER_FLAGS* alter_flags);
	virtual int		alter_table_phase3(THD* thd, TABLE* altered_table);
	
#ifdef TRUNCATE_ENABLED
	virtual int		delete_all_rows(void);
#endif

	int				addColumn(THD* thd, TABLE* altered_table, HA_CREATE_INFO* create_info, HA_ALTER_INFO* alter_info, HA_ALTER_FLAGS* alter_flags);
	int				addIndex(THD* thd, TABLE* alteredTable, HA_CREATE_INFO* createInfo, HA_ALTER_INFO* alterInfo, HA_ALTER_FLAGS* alterFlags);
	int				dropIndex(THD* thd, TABLE* alteredTable, HA_CREATE_INFO* createInfo, HA_ALTER_INFO* alterInfo, HA_ALTER_FLAGS* alterFlags);

	void			getDemographics(void);
	int				createIndex(const char *schemaName, const char *tableName, TABLE *table, int indexId);
	int				dropIndex(const char *schemaName, const char *tableName, TABLE *table, int indexId, bool online);
	void			getKeyDesc(TABLE *table, int indexId, StorageIndexDesc *indexInfo);
	void			startTransaction(void);
	bool			threadSwitch(THD *newThread);
	int				threadSwitchError(void);
	int				error(int storageError);
	void			freeActiveBlobs(void);
	int				setIndex(TABLE *table, int indexId);
	int				setIndexes(TABLE *table);
	int				remapIndexes(TABLE *table);
	bool			validateIndexes(TABLE *table, bool exclusiveLock = false);
	int				genTable(TABLE* table, CmdGen* gen);
	int				genType(Field *field, CmdGen *gen);
	void			genKeyFields(KEY *key, CmdGen *gen);
	void			encodeRecord(uchar *buf, bool updateFlag);
	void			decodeRecord(uchar *buf);
	void			unlockTable(void);
	void			checkBinLog(void);
	void			mapFields(TABLE *table);
	void			unmapFields(void);

	static StorageConnection* getStorageConnection(THD* thd);
	
	static int		falcon_init(void *p);
	static int		falcon_deinit(void *p);
	static int		commit(handlerton *, THD *thd, bool all);
	static int		prepare(handlerton* hton, THD* thd, bool all);
	static int		rollback(handlerton *, THD *thd, bool all);
	static int		recover (handlerton * hton, XID *xids, uint length);
	static int		savepointSet(handlerton *, THD *thd, void *savePoint);
	static int		savepointRollback(handlerton *, THD *thd, void *savePoint);
	static int		savepointRelease(handlerton *, THD *thd, void *savePoint);
	static void		dropDatabase(handlerton *, char *path);
	static void		shutdown(handlerton *);
	static int		closeConnection(handlerton *, THD *thd);
	static void		logger(int mask, const char *text, void *arg);
	static int		panic(handlerton* hton, ha_panic_function flag);
	//static bool	show_status(handlerton* hton, THD* thd, stat_print_fn* print, enum ha_stat_type stat);
	static int		getMySqlError(int storageError);
	
#if 0
	static uint		alter_table_flags(uint flags);
#endif
	static int		alter_tablespace(handlerton* hton, THD* thd, st_alter_tablespace* ts_info);
	static int		fill_is_table(handlerton *hton, THD *thd, TABLE_LIST *tables, class Item *cond, enum enum_schema_tables);

	static int		commit_by_xid(handlerton* hton, XID* xid);
	static int		rollback_by_xid(handlerton* hton, XID* xid);
	static int		start_consistent_snapshot(handlerton *, THD *thd);

	static void		updateRecordMemoryMax(MYSQL_THD thd, struct st_mysql_sys_var* variable, void* var_ptr, const void* save);
	static void		updateRecordScavengeThreshold(MYSQL_THD thd, struct st_mysql_sys_var* variable, void* var_ptr, const void* save);
	static void		updateRecordScavengeFloor(MYSQL_THD thd, struct st_mysql_sys_var* variable, void* var_ptr, const void* save);
	static void		updateDebugMask(MYSQL_THD thd, struct st_mysql_sys_var* variable, void* var_ptr, const void* save);

	/* Turn off table cache for now */

	//uint8 table_cache_type() { return HA_CACHE_TBL_TRANSACT; }

	StorageConnection*	storageConnection;
	StorageTable*		storageTable;
	StorageTableShare*	storageShare;
	Field					**fieldMap;
	const char*			errorText;
	THR_LOCK_DATA		lockData;			// MySQL lock
	THD					*mySqlThread;
	TABLE_SHARE			*share;
	uint					recordLength;
	int					lastRecord;
	int					nextRecord;
	int					indexErrorId;
	int					errorKey;
	int					maxFields;
	StorageBlob			*activeBlobs;
	StorageBlob			*freeBlobs;
	bool					haveStartKey;
	bool					haveEndKey;
	bool					tableLocked;
	bool					tempTable;
	bool					lockForUpdate;
	bool					indexOrder;
	key_range			startKey;
	key_range			endKey;
	uint64				insertCount;
	ulonglong			tableFlags;
};

class NfsPluginHandler
{
public:
	NfsPluginHandler(void);
	~NfsPluginHandler(void);

	StorageConnection	*storageConnection;
	StorageTable		*storageTable;

	static int getSystemMemoryDetailInfo(THD *thd, TABLE_LIST *tables, COND *cond);
	static int initSystemMemoryDetailInfo(void *p);
	static int deinitSystemMemoryDetailInfo(void *p);
	
	static int getSystemMemorySummaryInfo(THD *thd, TABLE_LIST *tables, COND *cond);
	static int initSystemMemorySummaryInfo(void *p);
	static int deinitSystemMemorySummaryInfo(void *p);

	static int getRecordCacheDetailInfo(THD *thd, TABLE_LIST *tables, COND *cond);
	static int initRecordCacheDetailInfo(void *p);
	static int deinitRecordCacheDetailInfo(void *p);

	static int getRecordCacheSummaryInfo(THD *thd, TABLE_LIST *tables, COND *cond);
	static int initRecordCacheSummaryInfo(void *p);
	static int deinitRecordCacheSummaryInfo(void *p);

	static int getTableSpaceIOInfo(THD *thd, TABLE_LIST *tables, COND *cond);
	static int initTableSpaceIOInfo(void *p);
	static int deinitTableSpaceIOInfo(void *p);

	static int getTransactionInfo(THD *thd, TABLE_LIST *tables, COND *cond);
	static int initTransactionInfo(void *p);
	static int deinitTransactionInfo(void *p);

	static int getTransactionSummaryInfo(THD *thd, TABLE_LIST *tables, COND *cond);
	static int initTransactionSummaryInfo(void *p);
	static int deinitTransactionSummaryInfo(void *p);

	static int getSerialLogInfo(THD *thd, TABLE_LIST *tables, COND *cond);
	static int initSerialLogInfo(void *p);
	static int deinitSerialLogInfo(void *p);

	static int getFalconVersionInfo(THD *thd, TABLE_LIST *tables, COND *cond);
	static int initFalconVersionInfo(void *p);
	static int deinitFalconVersionInfo(void *p);

	static int getSyncInfo(THD *thd, TABLE_LIST *tables, COND *cond);
	static int initSyncInfo(void *p);
	static int deinitSyncInfo(void *p);
};
