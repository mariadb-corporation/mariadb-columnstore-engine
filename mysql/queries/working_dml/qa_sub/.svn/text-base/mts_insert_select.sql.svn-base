drop table if exists t1;
drop table if exists t2;
drop table if exists t3;

create table t1 (bandID INT, payoutID SMALLINT) engine=infinidb;
insert into t1 (bandID,payoutID) VALUES (1,6),(2,6),(3,4),(4,9),(5,10),(6,1),(7,12),(8,12);
create table t2 (payoutID SMALLINT) engine=infinidb;
insert into t2 (payoutID) SELECT DISTINCT payoutID FROM t1;
select * from t2 order by payoutID;
insert into t2 (payoutID) SELECT payoutID+10 FROM t1;
select * from t2 order by payoutID;
insert ignore into t2 (payoutID) SELECT payoutID+10 FROM t1;
select * from t2 order by payoutID;
drop table t1;
drop table t2;




CREATE TABLE `t1` (
  `numeropost` bigint(20),
  `icone` tinyint(4),
  `numreponse` bigint(20),
  `contenu` char(20),
  `pseudo` varchar(50),
  `date1` datetime,
  `ip` bigint(11),
  `signature` tinyint(1)) ENGINE=infinidb;

CREATE TABLE `t2` (
  `numeropost` bigint(20),
  `icone` tinyint(4),
  `numreponse` bigint(20),
  `contenu` char(20),
  `pseudo` varchar(50),
  `date1` datetime,
  `ip` bigint(11),
  `signature` tinyint(1)) ENGINE=infinidb;

INSERT INTO t2
(numeropost,icone,numreponse,contenu,pseudo,date1,ip,signature) VALUES
(9,1,56,'test','joce','2001-07-25 13:50:53'
,3649052399,0);
select * from t2 order by numeropost;
INSERT INTO t1 (numeropost,icone,contenu,pseudo,date1,signature,ip)
SELECT 1618,icone,contenu,pseudo,date1,signature,ip FROM t2
WHERE numeropost=9 ORDER BY numreponse ASC;
select * from t1 order by numeropost;
INSERT INTO t1 (numeropost,icone,contenu,pseudo,date1,signature,ip)
SELECT 1718,icone,contenu,pseudo,date1,signature,ip FROM t2
WHERE numeropost=9 ORDER BY numreponse ASC;
select * from t1 order by numeropost;
DROP TABLE t1;
drop table t2;




create table t1 (a int) engine=infinidb;
create table t2 (a int) engine=infinidb;
insert into t1 values (1);
insert into t1 values (2);
insert into t1 values (3);
insert into t1 values (4),(5);
insert into t1 select * from t1;
select * from t1 order by a;
insert into t1 select * from t1 as t2;
select * from t1 order by a;
insert into t2 select * from t1 as t2;
select * from t2 order by 1;
insert into t1 select t2.a from t2;
select * from t1 order by 1;
drop table t1;
drop table t2;




CREATE TABLE t1 ( USID INTEGER, ServerID TINYINT, State VARCHAR(20), SessionID CHAR(32), User1 CHAR(32), NASAddr INTEGER, NASPort INTEGER, NASPortType INTEGER, ConnectSpeed INTEGER, CarrierType CHAR(32), CallingStationID CHAR(32), CalledStationID CHAR(32), AssignedAddr INTEGER, SessionTime INTEGER, PacketsIn INTEGER, OctetsIn INTEGER, PacketsOut INTEGER, OctetsOut INTEGER, TerminateCause INTEGER, UnauthTime TINYINT, AccessRequestTime DATETIME, AcctStartTime DATETIME, AcctLastTime DATETIME, LastModification DATETIME) engine=infinidb;
CREATE TABLE t2 ( USID INTEGER, ServerID TINYINT, State VARCHAR(20), SessionID CHAR(32), User1 CHAR(32), NASAddr INTEGER, NASPort INTEGER, NASPortType INTEGER, ConnectSpeed INTEGER, CarrierType CHAR(32), CallingStationID CHAR(32), CalledStationID CHAR(32), AssignedAddr INTEGER, SessionTime INTEGER, PacketsIn INTEGER, OctetsIn INTEGER, PacketsOut INTEGER, OctetsOut INTEGER, TerminateCause INTEGER, UnauthTime TINYINT, AccessRequestTime DATETIME, AcctStartTime DATETIME, AcctLastTime DATETIME, LastModification DATETIME) engine=infinidb;
INSERT INTO t1 VALUES (39,42,'Access-Granted','46','491721000045',2130706433,17690,NULL,NULL,'Localnet','491721000045','49172200000',754974766,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'2003-07-18 00:11:21',NULL,NULL,20030718001121);
INSERT INTO t2 SELECT USID, ServerID, State, SessionID, User1, NASAddr, NASPort, NASPortType, ConnectSpeed, CarrierType, CallingStationID, CalledStationID, AssignedAddr, SessionTime, PacketsIn, OctetsIn, PacketsOut, OctetsOut, TerminateCause, UnauthTime, AccessRequestTime, AcctStartTime, AcctLastTime, LastModification from t1 LIMIT 1;
select * from t2 order by 1;
drop table t1;
drop table t2;




create table t1 (f1 int) engine=infinidb;
create table t2 (ff1 int, ff2 int) engine=infinidb;
insert into t1 values (1),(1),(2);
insert into t2(ff1) select f1 from t1 on duplicate key update ff2=ff2+1;
select * from t2 order by 1;
drop table t1;
drop table t2;




create table t1 (a int) engine=infinidb;
create table t2 (a int, b int) engine=infinidb;
create table t3 (c int, d int) engine=infinidb;
insert into t1 values (1),(2);
insert into t2 values (3,4);
insert into t3 values (5,6),(7,7);
select * from t1 order by 1;
insert into t1 select a from t2;
select * from t1 order by 1;
insert into t1 select a+1 from t2;
select * from t1 order by 1;
insert into t1 select t3.c from t3;
select * from t1 order by 1;
insert into t1 select t2.a from t2;
select * from t1 order by 1;
drop table t1;
drop table t2;
drop table t3;



create table t1(f1 varchar(5)) engine=infinidb;
insert into t1(f1) select if(max(f1) is null, '2000',max(f1)+1) from t1;
select * from t1 order by 1;
insert into t1(f1) select if(max(f1) is null, '2000',max(f1)+1) from t1;
select * from t1 order by 1;
insert into t1(f1) select if(max(f1) is null, '2000',max(f1)+1) from t1;
select * from t1 order by 1;
drop table t1;



create table t1(x int, y char(1)) engine=infinidb;
create table t2(x int, z int) engine=infinidb;
insert into t2 values (5,6);
select * from t2 order by x;
insert into t1(x,y) select x, cast(z as char(1)) from t2;
select * from t1 order by 1;
drop table t1;
drop table t2; 



CREATE TABLE t1 (a int) engine=infinidb;
INSERT INTO t1 values (1), (2);
INSERT INTO t1 SELECT a + 2 FROM t1 LIMIT 1;
select * from t1 order by 1;
DROP TABLE t1;







CREATE TABLE t1 (x int, y int) engine=infinidb;
CREATE TABLE t2 (z int, y int) engine=infinidb;
CREATE TABLE t3 (a int, b int) engine=infinidb;
insert into t1 values (1,2), (3,4);
insert into t2 values (1,2), (5,2), (6,3);
select * from t1 order by 1;
select * from t2 order by 1;
INSERT INTO t3 (SELECT x, y FROM t1 JOIN t2 USING (y) WHERE z = 1);
select * from t3 order by 1;
DROP TABLE t1;
drop table t2;
drop table t3;




CREATE TABLE t1 (f1 INT, f2 INT) engine=infinidb;
CREATE TABLE t2 (f1 INT, f2 INT) engine=infinidb;
INSERT INTO t1 VALUES (1,1),(2,2),(10,10);
select * from t1 order by f1;
INSERT INTO t2 (f1, f2) SELECT f1, f2 FROM t1;
select * from t2 order by 1;
INSERT INTO t2 (f1, f2) SELECT f1 as aa, f1 as bb FROM t2 src WHERE f1 < 2;
SELECT * FROM t2 order by 1;
DROP TABLE t1;
drop table t2;




CREATE TABLE t1 (c VARCHAR(30)) engine=infinidb;
CREATE TABLE t2 (d VARCHAR(10)) engine=infinidb; 
INSERT INTO t1 (c) VALUES ('7_chars'), ('13_characters'); 

SELECT (SELECT SUM(LENGTH(c)) FROM t1 WHERE c='13_characters') FROM t1 order by 1;

INSERT INTO t2 (d) 
  SELECT (SELECT SUM(LENGTH(c)) FROM t1 WHERE c='13_characters') FROM t1;
select * from t2 order by 1;

INSERT INTO t2 (d) 
  SELECT (SELECT SUM(LENGTH(c)) FROM t1 WHERE c='7_chars') FROM t1;
select * from t2 order by 1;

INSERT INTO t2 (d)
  SELECT (SELECT SUM(LENGTH(c)) FROM t1 WHERE c IN (SELECT t1.c FROM t1)) 
  FROM t1;
SELECT * FROM t2 order by 1;
DROP TABLE t1;
drop table t2;



CREATE DATABASE if not exists bug21774_1;
CREATE DATABASE if not exists bug21774_2;
CREATE TABLE bug21774_1.t1(id VARCHAR(10), label VARCHAR(255)) engine=infinidb;
CREATE TABLE bug21774_2.t1(id VARCHAR(10), label VARCHAR(255)) engine=infinidb;
insert into bug21774_1.t1 values ('abc', 'def');
select * from bug21774_1.t1  order by 1;
INSERT INTO bug21774_2.t1 SELECT t1.* FROM bug21774_1.t1;
select * from bug21774_2.t1 order by 1;
use bug21774_1;
INSERT INTO bug21774_2.t1 SELECT t1.* FROM t1;
select * from bug21774_2.t1 order by 1;
drop table t1;
drop table bug21774_2.t1; 
#USE tpch1;
DROP DATABASE bug21774_1;
DROP DATABASE bug21774_2;

