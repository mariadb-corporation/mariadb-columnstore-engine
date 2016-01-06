
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
drop table if exists t4;

CREATE TABLE `t1` ( `id` int(9), `taskid` bigint(20) , `dbid` int(11), `create_date` datetime, `last_update` datetime) ENGINE=infinidb;
INSERT INTO `t1` (`id`, `taskid`, `dbid`, `create_date`,`last_update`) VALUES (1, 1, 15, '2003-09-29 10:31:36', '2003-09-29 10:31:36'), (2, 1, 21, now(), now());

CREATE TABLE `t2` (`db_id` int(11), `name` varchar(200), `primary_uid` smallint(6), `secondary_uid` smallint(6)) ENGINE=infinidb;
INSERT INTO `t2` (`db_id`, `name`, `primary_uid`, `secondary_uid`) VALUES (18, 'Not Set 1', 0, 0),(19, 'Valid', 1, 2),(20, 'Valid 2', 1, 2),(21, 'Should Not Return', 1, 2),(26, 'Not Set 2', 0, 0),(-1, 'ALL DB\'S', 0, 0);

CREATE TABLE `t3` (`taskgenid` int(9), `dbid` int(11), `taskid` int(11), `mon` tinyint(4), `tues` tinyint(4), `wed` tinyint(4), `thur` tinyint(4), `fri` tinyint(4), `sat` tinyint(4), `sun` tinyint(4), `how_often` smallint(6), `userid` smallint(6), `active` tinyint(4)) ENGINE=infinidb;
INSERT INTO `t3` (`taskgenid`, `dbid`, `taskid`, `mon`, `tues`,`wed`, `thur`, `fri`, `sat`, `sun`, `how_often`, `userid`, `active`) VALUES (1,-1, 1, 1, 1, 1, 1, 1, 0, 0, 1, 0, 1);

CREATE TABLE `t4` (`task_id` smallint(6),`description` varchar(200)) ENGINE=infinidb;
INSERT INTO `t4` (`task_id`, `description`) VALUES (1, 'Daily Check List'),(2, 'Weekly Status');

# WWW.  Added alias to long column name below.
# ERROR 138 (HY000) at line 20: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select  dbid, name, (date_format(now() , '%Y-%m-%d') - INTERVAL how_often DAY) >= ifnull((SELECT date_format(max(create_date),'%Y-%m-%d') FROM t1 WHERE dbid = b.db_id AND taskid = a.taskgenid), '1950-01-01') longCol from t3 a, t2 b, t4  WHERE dbid = - 1 AND primary_uid = '1' AND t4.task_id = taskid;

SELECT dbid, name FROM t3 a, t2 b, t4 WHERE dbid = - 1 AND primary_uid = '1' AND ((date_format(now() , '%Y-%m-%d') - INTERVAL how_often DAY) >= ifnull((SELECT date_format(max(create_date),'%Y-%m-%d') FROM t1 WHERE dbid = b.db_id AND taskid = a.taskgenid), '1950-01-01')) AND t4.task_id = taskid order by 1, 2;
drop table t1;
drop table t2;
drop table t3;
drop table t4;

CREATE TABLE t1 (id int(11));
INSERT INTO t1 VALUES (1),(5);
CREATE TABLE t2 (id int(11));
INSERT INTO t2 VALUES (2),(6),(5);
select * from t1 where (1) in (select id from t2);
DROP TABLE t1;
drop table t2;

CREATE TABLE t1 (number char(11)) ENGINE=infinidb;
INSERT INTO t1 VALUES ('69294728265'),('18621828126'),('89356874041'),('95895001874');
CREATE TABLE t2 (code char(5)) ENGINE=infinidb;
INSERT INTO t2 VALUES ('1'),('1226'),('1245'),('1862'),('18623'),('1874'),('1967'),('6');
# ERROR 138 (HY000) at line 37: IDB-1000: 'c' is not joined.
# select c.number as phone,(select p.code from t2 p where c.number like concat(p.code, '%') order by length(p.code) desc limit 1) as code from t1 c;
drop table t1;
drop table t2;

CREATE TABLE t1(COLA FLOAT, COLB FLOAT, COLC VARCHAR(20)) engine=infinidb;
CREATE TABLE t2(COLA FLOAT, COLB FLOAT, COLC CHAR(1)) engine=infinidb;
INSERT INTO t1 VALUES (1,1,'1A3240'), (1,2,'4W2365');
INSERT INTO t2 VALUES (100, 200, 'C');
SELECT DISTINCT COLC FROM t1 WHERE COLA = (SELECT COLA FROM t2 WHERE COLB = 200 AND COLC ='C' LIMIT 1);
DROP TABLE t1;
drop table t2;

CREATE TABLE t1 (a int(1)) engine=infinidb;
INSERT INTO t1 VALUES (1),(1),(1),(1),(1),(2),(3),(4),(5);
# ERROR 138 (HY000) at line 58: IDB-2028: FROM keyword not found where expected.
# SELECT DISTINCT (SELECT a) FROM t1;
DROP TABLE t1;

CREATE TABLE `t1` (
  `id` int(11),
  `id_cns` tinyint(3),
  `tipo` char(3),
  `anno_dep` smallint(4),
  `particolare` int(8),
  `generale` int(8),
  `bis` tinyint(3)) engine=infinidb;
INSERT INTO `t1` VALUES (1,16,'UNO',1987,2048,9681,0),(2,50,'UNO',1987,1536,13987,0),(3,16,'UNO',1987,2432,14594,0),(4,16,'UNO',1987,1792,13422,0),(5,16,'UNO',1987,1025,10240,0),(6,16,'UNO',1987,1026,7089,0);

CREATE TABLE `t2` (
  `id` tinyint(3),
  `max_anno_dep` smallint(6)
) engine=infinidb;
INSERT INTO `t2` VALUES (16,1987),(50,1990),(51,1990);

# ERROR 138 (HY000) at line 78: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# SELECT cns.id, cns.max_anno_dep, cns.max_anno_dep = (SELECT s.anno_dep FROM t1 AS s WHERE s.id_cns = cns.id ORDER BY s.anno_dep DESC LIMIT 1) AS PIPPO FROM t2 AS cns;

DROP TABLE t1;
drop table t2;

create table t1 (a int) engine=infinidb;
insert into t1 values (1), (2), (3);
select sum(a) from (select * from t1 limit 1) as a;
# ERROR 138 (HY000) at line 4: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 2 in (select * from t1);
drop table t1;

CREATE TABLE t1 (a int, b int) engine=infinidb;
INSERT INTO t1 VALUES (1, 1), (1, 2), (1, 3);
SELECT * FROM t1 WHERE a = (SELECT MAX(a) FROM t1 WHERE a = 1) ORDER BY b;
DROP TABLE t1;

create table t1(val varchar(10)) engine=infinidb;
insert into t1 values ('aaa'), ('bbb'),('eee'),('mmm'),('ppp');
select count(*) from t1 as w1 where w1.val in (select w2.val from t1 as w2 where w2.val like 'm%') and w1.val in (select w3.val from t1 as w3 where w3.val like 'e%');
drop table t1;


