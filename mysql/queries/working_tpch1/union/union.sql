# Script from Mark.  Copied on 12/16 from \\calweb\QA\TestCycles\Iteration_0950\Functional\union.
# Edits:
# 1) Commented out 3 queries that have mixed join cases which are currently not supported.
# 2) Added order bys.

drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
drop table if exists t4;
drop table if exists t5;
drop table if exists t6;

CREATE TABLE t1 (a int, b char (10)) engine=infinidb;
insert into t1 values(1,'a'),(2,'b'),(3,'c'),(3,'c');
CREATE TABLE t2 (a int, b char (10)) engine=infinidb;
insert into t2 values (3,'c'),(4,'d'),(5,'f'),(6,'e');

select a,b from t1 union distinct select a,b from t2 order by 1, 2;
select a,b from t1 union all select a,b from t2 order by 1, 2;
select a,b from t1 union all select a,b from t2 order by b, a;

select a,b from t1 union all select a,b from t2 union select 7,'g' order by 1, 2;
select 0,'#' union select a,b from t1 union all select a,b from t2 union select 7,'gg'order by 1, 2;
select a,b from t1 union select a,b from t1 order by 1, 2;
select 't1',b,count(*) from t1 group by b UNION select 't2',b,count(*) from t2 group by b order by 1, 2, 3;

(select a,b from t1)  union all (select a,b from t2 order by a) order by 1, 2;
(select a,b from t1 )  union all (select a,b from t2 order by a ) order by 1, 2;
(select a,b from t1 )  union all (select a,b from t2 order by a ) order by b desc, a;
(select a,b from t1 )  union all (select a,b from t2 order by a ) order by a, b;

select a,b from t1 union select a,b from t2 order by 1, 2;

select * from t1 union select * from t2 order by 1, 2;

drop table t1;
drop table t2;


CREATE TABLE t1 (
  `pseudo` char(35) ,
  `pseudo1` char(35) ,
  `same` tinyint(1)
) ENGINE=infinidb;

INSERT INTO t1 (pseudo,pseudo1,same) VALUES ('joce', 'testtt', 1),('joce', 'tsestset', 1),('dekad', 'joce', 1);
SELECT pseudo FROM t1 WHERE pseudo1='joce' UNION SELECT pseudo FROM t1 WHERE pseudo='joce' order by 1;
SELECT pseudo1 FROM t1 WHERE pseudo1='joce' UNION SELECT pseudo1 FROM t1 WHERE pseudo='joce' order by 1;
SELECT * FROM t1 WHERE pseudo1='joce' UNION SELECT * FROM t1 WHERE pseudo='joce' order by pseudo desc,pseudo1 desc;
SELECT pseudo1 FROM t1 WHERE pseudo='joce' UNION SELECT pseudo FROM t1 WHERE pseudo1='joce' order by 1;
SELECT pseudo1 FROM t1 WHERE pseudo='joce' UNION ALL SELECT pseudo FROM t1 WHERE pseudo1='joce' order by 1;
SELECT pseudo1 FROM t1 WHERE pseudo='joce' UNION SELECT 1 order by 1;
drop table t1;

create table t1 (a int) engine=infinidb;
create table t2 (a int) engine=infinidb;
insert into t1 values (1),(2),(3),(4),(5);
insert into t2 values (11),(12),(13),(14),(15);
(select * from t1 ) union (select * from t2 ) order by 1;
(select * from t1 ) union (select * from t2 ) order by 1;
(select * from t1 ) union (select * from t2 ) order by 1;
(select * from t1 ) union (select * from t2 ) order by 1;
drop table t1;
drop table t2;

CREATE TABLE t1 (
  cid smallint(5) ,
  cv varchar(250)
) engine=infinidb;

INSERT INTO t1 VALUES (8,'dummy');
CREATE TABLE t2 (
  cid bigint(20),
  cap varchar(255)
) engine=infinidb;
CREATE TABLE t3 (
  gid bigint(20) ,
  gn varchar(255),
  must tinyint(4)
) engine=infinidb;
INSERT INTO t3 VALUES (1,'V1',NULL);
CREATE TABLE t4 (
  uid bigint(20),
  gid bigint(20),
  rid bigint(20),
  cid bigint(20)
) engine=infinidb;

INSERT INTO t4 VALUES (1,1,NULL,NULL);
CREATE TABLE t5 (
  rid bigint(20),
  rl varchar(255)
) engine=infinidb;
CREATE TABLE t6 (
  uid bigint(20),
  un varchar(250),
  uc smallint(5)
) engine=infinidb;
INSERT INTO t6 VALUES (1,'test',8);

SELECT t4.uid, t5.rl, t3.gn as g1, t4.cid, t4.gid as gg FROM t3, t6, t1, t4 left join t5 on t5.rid = t4.rid left join t2 on t2.cid = t4.cid WHERE t3.gid=t4.gid AND t6.uid = t4.uid AND t6.uc  = t1.cid AND t1.cv = "dummy" AND t6.un = "test";
SELECT t4.uid, t5.rl, t3.gn as g1, t4.cid, t4.gid as gg FROM t3, t6, t1, t4 left join t5 on t5.rid = t4.rid left join t2 on t2.cid = t4.cid WHERE t3.gid=t4.gid AND t6.uid = t4.uid AND t3.must IS NOT NULL AND t6.uc  = t1.cid AND t1.cv = "dummy" AND t6.un = "test";
(SELECT t4.uid, t5.rl, t3.gn as g1, t4.cid, t4.gid as gg FROM t3, t6, t1, t4 left join t5 on t5.rid = t4.rid left join t2 on t2.cid = t4.cid WHERE t3.gid=t4.gid AND t6.uid = t4.uid AND t3.must IS NOT NULL AND t6.uc  = t1.cid AND t1.cv = "dummy" AND t6.un = "test") UNION (SELECT t4.uid, t5.rl, t3.gn as g1, t4.cid, t4.gid as gg FROM t3, t6, t1, t4 left join t5 on t5.rid = t4.rid left join t2 on t2.cid = t4.cid WHERE t3.gid=t4.gid AND t6.uid = t4.uid AND t6.uc  = t1.cid AND t1.cv = "dummy" AND t6.un = "test");
drop table t1;
drop table t2;
drop table t3;
drop table t4;
drop table t5;
drop table t6;

create table t1 (a int) engine=infinidb;
insert into t1 values (1),(2),(3);
create table t2 (a int) engine=infinidb;
insert into t2 values (3),(4),(5);

SELECT * FROM t1 UNION SELECT * FROM t2 ORDER BY a desc;
/* Bug 2865.  Umcomment query below with resolution of bug 2865. */
SELECT * FROM t1 UNION SELECT * FROM t2 ORDER BY a desc limit 1;
(SELECT * FROM t1 ORDER by a) UNION ALL (SELECT * FROM t2 ORDER BY a) ORDER BY A desc LIMIT 4;

drop table t1;
drop table t2;


CREATE TABLE t1 (id int(3)) ENGINE=infinidb;
INSERT INTO t1 (id) VALUES("1");
CREATE TABLE t2 ( id int(3) ,  id_master int(5) ,  text1 varchar(5),  text2 varchar(5)) ENGINE=infinidb;
INSERT INTO t2 (id, id_master, text1, text2) VALUES("1", "1", "foo1", "bar1");
INSERT INTO t2 (id, id_master, text1, text2) VALUES("2", "1", "foo2", "bar2");
INSERT INTO t2 (id, id_master, text1, text2) VALUES("3", "1", NULL, "bar3");
INSERT INTO t2 (id, id_master, text1, text2) VALUES("4", "1", "foo4", "bar4");

SELECT 1 AS id_master, 1 AS id, NULL AS text1, 'ABCDE' AS text2 UNION SELECT id_master, t2.id, text1, text2 FROM t1 LEFT JOIN t2 ON t1.id = t2.id_master order by 1, 2, 3, 4;
SELECT 1 AS id_master, 1 AS id, 'ABCDE' AS text1, 'ABCDE' AS text2 UNION SELECT id_master, t2.id, text1, text2 FROM t1 LEFT JOIN t2 ON t1.id = t2.id_master order by 1, 2, 3, 4;
drop table if exists t1;
drop table t2;


create table t1 (a int , b int) engine=infinidb;
create table t2 (a int , b int) engine=infinidb;
insert into t1  values (1,1),(2,2),(5,2),(6,3);
insert into t2  values (1,10),(2,11),(3,12),(4,13);

(select * from t1 where a=5) union (select * from t2 where a=1) order by 1, 2;
(select * from t1 where a=5 and a=6) union (select * from t2 where a=1) order by 1, 2;
(select t1.a,t1.b from t1,t2 where t1.a=5 and t1.b = t2.b) union (select * from t2 where a=1) order by 1, 2;
(select * from t1 where a=1) union (select t1.a,t2.a from t1,t2 where t1.a=t2.a) order by 1, 2;
drop table t1;
drop table t2;

create table t1 (   id int, user_name char(10) ) engine=infinidb;
create table t2 (    id int, group_name char(10) ) engine=infinidb;
create table t3 (    id int, user_id int ,group_id int ) engine=infinidb;
insert into t1 (user_name) values ('Tester');
insert into t1 values (1, 'valid test');
insert into t2 (group_name) values ('Group A');
insert into t2 values (1, 'Group C');
insert into t2 (group_name) values ('Group B');
insert into t3 (user_id, group_id) values (1,1);
insert into t3 values (1, 1, 1);
select 1 'is_in_group', a.user_name, c.group_name, b.id from t1 a, t3 b, t2 c where a.id = b.user_id and b.group_id = c.id UNION  select 0 'is_in_group', a.user_name, c.group_name, null from t1 a, t2 c where a.id = c.id order by 1, 2, 3, 4;
drop table t1;
drop table t2;
drop table t3;

create table t1 (mat_id INT, matintnum CHAR(6), test INT ) engine=infinidb;
create table t2 (mat_id INT, pla_id INT ) engine=infinidb;
insert into t1 values (NULL, 'a', 1), (NULL, 'b', 2), (NULL, 'c', 3), (NULL, 'd', 4), (NULL, 'e', 5), (NULL, 'f', 6), (NULL, 'g', 7), (NULL, 'h', 8), (NULL, 'i', 9);
insert into t2 values (1, 100), (1, 101), (1, 102), (2, 100), (2, 103), (2, 104), (3, 101), (3, 102), (3, 105);
SELECT mp.pla_id, MIN(m1.matintnum) AS matintnum FROM t2 mp INNER JOIN t1 m1 ON mp.mat_id=m1.mat_id GROUP BY mp.pla_id union SELECT 0, 0 order by 1, 2;
drop table t1;
drop table t2;

create table t1 (col1 tinyint, col2 tinyint) engine=infinidb;
insert into t1 values (1,2),(3,4),(5,6),(7,8),(9,10);
select col1 n from t1 union select col2 n from t1 order by n;
drop  table t1;

create table t1 (i int) engine=infinidb;
insert into t1 values (1);
select * from t1 UNION select * from t1 order by 1;
select * from t1 UNION ALL select * from t1 order by 1;
select * from t1 UNION select * from t1 UNION ALL select * from t1 order by 1;
drop table t1;

select 1 as a union all select 1 union all select 2 union select 1 union all select 2 order by 1;
select 1 union select 2 order by 1;
(select 1) union (select 2) order by 1;
(select 1) union (select 2) union (select 3) order by 1;

create table t1 (a int) engine=infinidb;
insert into t1 values (100), (1);
create table t2 (a int) engine=infinidb;
insert into t2 values (100);
select a from t1 union select a from t2 order by a;
select a from t1 union select a from t2 order by a;
drop table t1;
drop table t2;


create table t1 ( id int, col1 int) engine=infinidb;
insert into t1 (col1) values (2),(3),(4),(5),(6);
select 99 union all select id from t1 order by 1;
select id from t1 union all select 99 order by 1;
drop table t1;

(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) union
(select avg(1)) union (select avg(1)) union (select avg(1)) order by 1;

(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union
(select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) union (select avg(n_nationkey) from nation) order by 1;



CREATE TABLE t1 (a int) engine=infinidb;
INSERT INTO t1 VALUES (10), (20);
CREATE TABLE t2 (b int) engine=infinidb;
INSERT INTO t2 VALUES (10), (50), (50);

SELECT a,1 FROM t1 
UNION
SELECT b, COUNT(*) FROM t2 GROUP BY b ORDER BY a;

SELECT a,1 FROM t1 
UNION
SELECT b, COUNT(*) FROM t2 GROUP BY b ORDER BY a DESC;

SELECT a,1 FROM t1 
UNION
SELECT b, COUNT(*) FROM t2 GROUP BY b ORDER BY a ASC ;

SELECT a,1 FROM t1 
UNION ALL 
SELECT b, COUNT(*) FROM t2 GROUP BY b ORDER BY a DESC;

-- bug 4020
select a from ( select a from (select a from t1 union select a from t1) as s1) as s2;
select a from ( select a from (select a from t1 union all select a from t1) as s1) as s2;
select b from ( select b from (select b from t2 union select b from t2) as s1) as s2;
select b from ( select b from (select b from t2 union all select b from t2) as s1) as s2;

drop table t1;
drop table t2;
