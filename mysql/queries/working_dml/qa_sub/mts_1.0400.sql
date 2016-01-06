drop table if exists t1;
drop table if exists t2;

create table t1 (a int) engine=infinidb;
insert into t1 values (1),(2),(3);
# ERROR 138 (HY000) at line 6: IDB-3011: Non-supported item in Order By clause.
# (select * from t1) union (select * from t1) order by (select a from t1 limit 1);
drop table t1;

CREATE TABLE t1 (field char(1)) engine=infinidb;
INSERT INTO t1 VALUES ('b');
SELECT field FROM t1 WHERE 1 in (SELECT 1 UNION ALL SELECT 1 FROM (SELECT 1) a HAVING field='b');
drop table t1;

CREATE TABLE t1 (a int(1)) engine=infinidb;
INSERT INTO t1 VALUES (1);
# ERROR 138 (HY000) at line 16: IDB-3009: Unknown column 'a'.
# SELECT 1 FROM (SELECT a FROM t1) b HAVING (SELECT b.a)=1;
drop table t1;


CREATE TABLE t2 (id int(11)) ENGINE=infinidb;
INSERT INTO t2 VALUES (1),(2);
SELECT * FROM t2 WHERE id IN (SELECT 1);
# ERROR 138 (HY000) at line 23: IDB-3013: Subquery with union is currently only supported in the FROM clause.
# SELECT * FROM t2 WHERE id IN (SELECT 1 UNION SELECT 3);
SELECT * FROM t2 WHERE id IN (SELECT 1+(select 1));
# ERROR 138 (HY000) at line 25: IDB-3013: Subquery with union is currently only supported in the FROM clause.
# SELECT * FROM t2 WHERE id IN (SELECT 5 UNION SELECT 3);
# ERROR 138 (HY000) at line 26: IDB-3013: Subquery with union is currently only supported in the FROM clause.
# SELECT * FROM t2 WHERE id IN (SELECT 5 UNION SELECT 2);
drop table t2;


