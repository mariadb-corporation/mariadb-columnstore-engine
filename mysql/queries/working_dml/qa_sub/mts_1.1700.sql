drop table if exists t1;
drop table if exists t2;
drop table if exists t3;

create table t1 (a int) engine=infinidb;
insert into t1 values (1), (2), (3);
SELECT 1 FROM t1 WHERE (SELECT 1) in (SELECT 1);
drop table t1;

CREATE TABLE `t1` (
  `itemid` bigint(20),
  `sessionid` bigint(20),
  `time` int(10),
  `type` char(1),
  `data` char(1)
) engine=infinidb;
INSERT INTO `t1` VALUES (1, 1, 1, 'D', NULL);

# Changed column named date to xdate below.
CREATE TABLE `t2` (
  `sessionid` bigint(20),
  `pid` int(10),
  `xdate` int(10),
  `ip` varchar(15)
) engine=infinidb;
INSERT INTO `t2` VALUES (1, 1, 1, '10.10.10.1');
SELECT s.ip, count( e.itemid ) FROM `t1` e JOIN t2 s ON s.sessionid = e.sessionid WHERE e.sessionid = ( SELECT sessionid FROM t2 ORDER BY sessionid DESC LIMIT 1 ) GROUP BY s.ip HAVING count( e.itemid ) >0 LIMIT 0 , 30;
drop table t1;
drop table t2;

CREATE TABLE t1 (a char(5), b char(5)) engine=infinidb;
INSERT INTO t1 VALUES (NULL,'aaa'), ('aaa','aaa');
# ERROR 138 (HY000) at line 33: IDB-2024: Multiple columns with IN function is currently not supported.
# SELECT * FROM t1 WHERE (a,b) IN (('aaa','aaa'), ('aaa','bbb'));
DROP TABLE t1;

CREATE TABLE t1 (a int) engine=infinidb;
CREATE TABLE t2 (a int, b int) engine=infinidb;
CREATE TABLE t3 (b int) engine=infinidb;
INSERT INTO t1 VALUES (1), (2), (3), (4);
INSERT INTO t2 VALUES (1,10), (3,30);

SELECT * FROM t2 LEFT JOIN t3 ON t2.b=t3.b
  WHERE t3.b IS NOT NULL OR t2.a > 10;
SELECT * FROM t1
   WHERE t1.a NOT IN (SELECT a FROM t2 LEFT JOIN t3 ON t2.b=t3.b
                        WHERE t3.b IS NOT NULL OR t2.a > 10);

DROP TABLE t1;
drop table t2;
drop table t3;

create table t1 (retailerID varchar(8), statusID   int(10), changed    datetime) engine=infinidb;
INSERT INTO t1 VALUES("0026", "1", "2005-12-06 12:18:56");
INSERT INTO t1 VALUES("0026", "2", "2006-01-06 12:25:53");
INSERT INTO t1 VALUES("0037", "1", "2005-12-06 12:18:56");
INSERT INTO t1 VALUES("0037", "2", "2006-01-06 12:25:53");
INSERT INTO t1 VALUES("0048", "1", "2006-01-06 12:37:50");
INSERT INTO t1 VALUES("0059", "1", "2006-01-06 12:37:50");

select * from t1 r1 
 where (r1.retailerID,(r1.changed)) in 
        (SELECT r2.retailerId,(max(changed)) from t1 r2 
         group by r2.retailerId);
drop table t1;

CREATE TABLE t1 (                  
  field1 int,                 
  field2 int,            
  field3 int
) engine=infinidb;
CREATE TABLE t2 (             
  fieldA int,            
  fieldB int
) engine=infinidb; 

INSERT INTO t1 VALUES
  (1,1,1), (1,1,2), (1,2,1), (1,2,2), (1,2,3), (1,3,1);
INSERT INTO t2 VALUES (1,1), (1,2), (1,3);

SELECT field1, field2, COUNT(*)
  FROM t1 GROUP BY field1, field2 order by 1, 2;

# ERROR 138 (HY000): IDB-3015: Correlated subquery in HAVING clause is currently not supported.
# SELECT field1, field2
# FROM  t1
#    GROUP BY field1, field2
#      HAVING COUNT(*) >= (SELECT count(*) 
#                                FROM t2 WHERE fieldA = field1);

# ERROR 138 (HY000): IDB-3015: Correlated subquery in HAVING clause is currently not supported.
# SELECT field1, field2
#  FROM  t1
#    GROUP BY field1, field2
#      HAVING COUNT(*) < (SELECT count(*) 
#                               FROM t2 WHERE fieldA = field1);

DROP TABLE t1;
drop table t2;


CREATE TABLE t1 (a int) engine=infinidb;
INSERT INTO t1 VALUES (1), (2);

# ERROR 138 (HY000) at line 100: doSimpleFilter: Unhandled SimpleFilter.
# SELECT a FROM t1 WHERE (SELECT 1 FROM DUAL WHERE 1=0) > 0;
# ERROR 138 (HY000) at line 107: doSimpleFilter: Unhandled SimpleFilter.
# SELECT a FROM t1 WHERE (SELECT 1 FROM DUAL WHERE 1=0) IS NULL;

DROP TABLE t1;



