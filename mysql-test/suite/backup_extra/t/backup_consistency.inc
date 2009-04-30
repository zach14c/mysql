let $grammar= $RQG_HOME/conf/invariant.yy;
let $gendata= $RQG_HOME/conf/invariant.zz;
let $reporter= BackupAndRestoreInvariant;
let $validator= Invariant;

--source include/have_rqg.inc

--echo # Executing RQG test. See rqg.out in the temp dir for output.
--exec perl $RQG_HOME/gentest.pl --dsn=dbi:mysql:host=127.0.0.1:port=$MASTER_MYPORT:user=root:database=test --engine=$engine --grammar=$grammar --gendata=$gendata --threads=25 --duration=600 --reporter=$reporter --validator=$validator --seed=time >$MYSQL_TMP_DIR/rqg.out 2>&1

CREATE DATABASE test;
--exit
