-- Changed columns named date to xdate as date is not supported as a column name by IDB.
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
CREATE TABLE t1 (a int(1));
SELECT (SELECT RAND() FROM t1) FROM t1;
SELECT (SELECT ENCRYPT('test') FROM t1) FROM t1;
SELECT (SELECT BENCHMARK(1,1) FROM t1) FROM t1;
drop table t1;

CREATE TABLE `t1` (`mot` varchar(30), `topic` int(8), `xdate` date, `pseudo` varchar(35)) ENGINE=infinidb;
CREATE TABLE `t2` (`mot` varchar(30), `topic` int(8), `xdate` date, `pseudo` varchar(35)) ENGINE=infinidb;
CREATE TABLE `t3` (`numeropost` int(8), `maxnumrep` int(10)) ENGINE=infinidb;

INSERT INTO t1 VALUES ('joce','1',null,'joce'),('test','2',null,'test');
INSERT INTO t2 VALUES ('joce','1',null,'joce'),('test','2',null,'test');
INSERT INTO t3 VALUES (1,1);

SELECT DISTINCT topic FROM t2 WHERE NOT EXISTS (SELECT * FROM t3 WHERE numeropost=topic);

select * from t1;
# ERROR 122 (HY000) at line 23: IDB-3017: Subquery in DELETE statements is currently not supported.
# DELETE FROM t1 WHERE topic IN (SELECT DISTINCT topic FROM t2 WHERE NOT EXISTS(SELECT * FROM t3 WHERE numeropost=topic));
select * from t1;

drop table t1;
drop table t2;
drop table t3;

