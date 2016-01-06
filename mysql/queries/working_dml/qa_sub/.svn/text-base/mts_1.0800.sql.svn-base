drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
CREATE TABLE t1 (a int(11)) engine=infinidb;
# WWW - edit to create table below.  Took out default and index and set engine to infinidb.
# CREATE TABLE t2 (a int(11) default '0', INDEX (a));
CREATE TABLE t2 (a int(11)) engine=infinidb;
INSERT INTO t1 VALUES (1),(2),(3),(4);
INSERT INTO t2 VALUES (1),(2),(3);
# ERROR 138 (HY000) at line 10: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# SELECT t1.a as a1, t1.a in (select t2.a from t2) as a2 FROM t1;

CREATE TABLE t3 (a int(11)) engine=infinidb;
INSERT INTO t3 VALUES (1),(2),(3);
# ERROR 138 (HY000) at line 15: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# SELECT t1.a as a1, t1.a in (select t2.a from t2,t3 where t3.a=t2.a) as a2 FROM t1;
drop table t1;
drop table t2;
drop table t3;

create table t1 (a float) engine=infinidb;
insert into t1 values (10.5);
# ERROR 138 (HY000) at line 24: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 10.5 IN (SELECT * from t1);
# ERROR 138 (HY000) at line 27: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 10.5 IN (SELECT * from t1 UNION SELECT 1.5);
drop table t1;

