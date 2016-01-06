drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
drop table if exists t4;

CREATE TABLE t4 (f7 varchar(32), f10 varchar(32)) engine=infinidb;
INSERT INTO t4 VALUES(1,1), (2,null);

CREATE TABLE t2 (f4 varchar(32), f2 varchar(50), f3 varchar(10)) engine=infinidb;
INSERT INTO t2 VALUES(1,1,null), (2,2,null);

CREATE TABLE t1 (f8 varchar(32), f1 varchar(10), f9 varchar(32)) engine=infinidb;
INSERT INTO t1 VALUES (1,'P',1), (2,'P',1), (3,'R',2);

CREATE TABLE t3 (f6 varchar(32), f5 varchar(50)) engine=infinidb;
INSERT INTO t3 VALUES (1,null), (2,null);

# ERROR 138 (HY000) at line 18: IDB-2016: Non supported item on the GROUP BY list.
/*
SELECT
  IF(t1.f1 = 'R', a1.f2, t2.f2) AS a4,
  IF(t1.f1 = 'R', a1.f3, t2.f3) AS f3,
  SUM(
    IF(
      (SELECT VPC.f2
       FROM t2 VPC, t4 a2, t2 a3
       WHERE
         VPC.f4 = a2.f10 AND a3.f2 = a4
       LIMIT 1) IS NULL, 
       0, 
       t3.f5
    )
  ) AS a6
FROM 
  t2, t3, t1 JOIN t2 a1 ON t1.f9 = a1.f4
GROUP BY a4;
*/

DROP TABLE t1;
drop table t2;
drop table t3;
drop table t4;

# WWW. Edited float declarations below.
# create table t1 (a float(5,4)) engine=infinidb;
# create table t2 (a float(5,4),b float(2,0)) engine=infinidb;
create table t1 (a float) engine=infinidb;
create table t2 (a float,b float) engine=infinidb;

insert into t1 values (1.0), (2.0);
insert into t2 values (1.0, 4.0), (2.0, 5.0);

select t1.a from t1 where   
  t1.a= (select b from t2 limit 1) and not
  t1.a= (select a from t2 limit 1) ;

drop table t1;
drop table t2;


CREATE TABLE t1 (a INT) engine=infinidb;
INSERT INTO t1 VALUES (1),(2);
SELECT 1 FROM t1 WHERE 1 IN (SELECT 1 FROM t1 GROUP BY a);
SELECT 1 FROM t1 WHERE 1 IN (SELECT 1 FROM t1 WHERE a > 3 GROUP BY a);
DROP TABLE t1;


CREATE TABLE t1(c int) engine=infinidb;
CREATE TABLE t2(a int, b int) engine=infinidb;
INSERT INTO t2 VALUES (1, 10), (2, NULL), (3, 2), (null, 3);
INSERT INTO t1 VALUES (1), (2), (null);

SELECT * FROM t2 WHERE b NOT IN (SELECT max(c) FROM t1 WHERE c > 1);

# Edited double drop below.
# DROP TABLE t1,t2;
drop table t1;
drop table t2;

