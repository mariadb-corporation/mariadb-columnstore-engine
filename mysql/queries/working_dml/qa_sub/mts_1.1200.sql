drop table if exists t1;
drop table if exists t2;
CREATE TABLE `t1` (`id` int(8), `pseudo` varchar(35)) ENGINE=infinidb;
INSERT INTO t1 VALUES (1, 'test');
# ERROR 138 (HY000) at line 4: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# SELECT 0 IN (SELECT 1 FROM t1 a);
INSERT INTO t1 VALUES (2,'test1');
# ERROR 138 (HY000) at line 4: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# SELECT 0 IN (SELECT 1 FROM t1 a);
drop table t1;


CREATE TABLE t1 (id int(11)) ENGINE=infinidb;
INSERT INTO t1 VALUES (1),(1),(2),(2),(1),(3);
CREATE TABLE t2 (id int(11), name varchar(15)) ENGINE=infinidb;

INSERT INTO t2 VALUES (4,'vita'), (1,'vita'), (2,'vita'), (1,'vita');
update t2 set t2.name='lenka' where t2.id in (select id from t1);
select * from t2;
drop table t1;
drop table t2;

