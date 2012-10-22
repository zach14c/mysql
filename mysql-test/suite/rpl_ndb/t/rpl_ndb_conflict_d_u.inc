--echo ***
--echo *** conflicting timestamps on delete-update
--echo ***

--echo *** slave - cause conflict
--connection slave
--source include/ndb_conflict_info_init.inc
update t1 set b="slave  X=3", X=3 where a=1;

--echo *** master - cause conflict
--connection master
delete from t1 where a=1;
delete from t1 where a=2;

--echo *** slave - check data
--sync_slave_with_master
--connection slave
--source include/ndb_conflict_info.inc
select * from t1 order by a;