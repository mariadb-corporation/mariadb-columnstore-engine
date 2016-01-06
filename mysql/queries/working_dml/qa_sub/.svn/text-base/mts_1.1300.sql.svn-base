drop table if exists t1;
drop table if exists t2;
drop table if exists t3;

create table t1 (a int) engine=infinidb; 
insert into t1 values (-1), (-4), (-2), (NULL); 
# ERROR 138 (HY000) at line 4: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select -10 IN (select a from t1); 
drop table t1;

create table t1 (id int, salary int) engine=infinidb;
insert into t1 values (1,100),(2,1000),(3,10000),(4,10),(5,500),(6,5000),(7,50000),(8,40000);
SELECT id FROM t1 where salary = (SELECT MAX(salary) FROM t1);
drop table t1;

CREATE TABLE t1 (
  ID int(10),
  SUB_ID int(3),
  REF_ID int(10),
  REF_SUB int(3)
) ENGINE=infinidb;
INSERT INTO t1 VALUES (1,0,NULL,NULL),(2,0,NULL,NULL);
SELECT DISTINCT REF_ID FROM t1 WHERE ID= (SELECT DISTINCT REF_ID FROM t1 WHERE ID=2);
DROP TABLE t1;

CREATE TABLE `t1` (
  `id` int(8),
  `pseudo` varchar(35),
  `email` varchar(60)) ENGINE=infinidb;
INSERT INTO t1 (id,pseudo,email) VALUES (1,'test','test'),(2,'test2','test2');
# ERROR 138 (HY000) at line 30: IDB-2016: Non supported item on the GROUP BY list.
# SELECT pseudo as a, pseudo as b FROM t1 GROUP BY (SELECT a) ORDER BY (SELECT id*1);
drop table if exists t1;

(SELECT 1 as a) UNION (SELECT 1) ORDER BY (SELECT a+0);

create table t1 (a int, b int) engine=infinidb;
create table t2 (a int) engine=infinidb;
create table t3 (a int, b int) engine=infinidb;
insert into t1 values (1,10), (2,20), (3,30),  (4,40);
insert into t2 values (2), (3), (4), (5);
insert into t3 values (10,3), (20,4), (30,5);
select * from t2 where t2.a in (select a from t1);
select * from t2 where t2.a in (select a from t1 where t1.b <> 30);
select * from t2 where t2.a in (select t1.a from t1,t3 where t1.b=t3.a);
drop table t1;
drop table t2;
drop table t3;

