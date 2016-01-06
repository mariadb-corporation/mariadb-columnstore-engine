drop table if exists t1;
drop table if exists t2;


CREATE TABLE t1 (a int) engine=infinidb;
INSERT INTO t1 VALUES (3), (1), (2);           
SELECT 'this is ' 'a test.' AS col1, a AS col2 FROM t1;
SELECT * FROM (SELECT 'this is ' 'a test.' AS col1, a AS t2 FROM t1) t;
DROP table t1;


CREATE TABLE t1 (a int, b INT, d INT, c CHAR(10)) engine=infinidb;
INSERT INTO t1 VALUES (1,1,0,'a'), (1,2,0,'b'), (1,3,0,'c'), (1,4,0,'d'),
(1,5,0,'e'), (2,1,0,'f'), (2,2,0,'g'), (2,3,0,'h'), (3,4,0,'i'), (3,3,0,'j'),
(3,2,0,'k'), (3,1,0,'l'), (1,9,0,'m'), (1,0,10,'n'), (2,0,5,'o'), (3,0,7,'p');

# ERROR 138 (HY000) at line 18: IDB-2026: Group function is not allowed in WHERE clause.
/*
SELECT a, MAX(b),
 (SELECT t.c FROM t1 AS t WHERE t1.a=t.a AND t.b=MAX(t1.b + 0)) as test 
 FROM t1 GROUP BY a;
*/
# ERROR 138 (HY000) at line 22: IDB-2026: Group function is not allowed in WHERE clause.
/*
SELECT a x, MAX(b),
 (SELECT t.c FROM t1 AS t WHERE x=t.a AND t.b=MAX(t1.b + 0)) as test
 FROM t1 GROUP BY a;
*/
# ERROR 138 (HY000) at line 26: IDB-2026: Group function is not allowed in WHERE clause.
/*
SELECT a, AVG(b),
  (SELECT t.c FROM t1 AS t WHERE t1.a=t.a AND t.b=AVG(t1.b)) AS test
  FROM t1 WHERE t1.d=0 GROUP BY a;
*/

# ERROR 138 (HY000) at line 33: IDB-3019: Limit within a correlated subquery is currently not supported.
/*
SELECT tt.a,
 (SELECT (SELECT c FROM t1 as t WHERE t1.a=t.a AND t.d=MAX(t1.b + tt.a)
 LIMIT 1) FROM t1 WHERE t1.a=tt.a GROUP BY a LIMIT 1) as test 
FROM t1 as tt;
*/

# ERROR 138 (HY000) at line 39: IDB-2026: Group function is not allowed in WHERE clause.
/*
SELECT tt.a,
(SELECT (SELECT t.c FROM t1 AS t WHERE t1.a=t.a AND t.d=MAX(t1.b + tt.a)
 LIMIT 1)
 FROM t1 WHERE t1.a=tt.a GROUP BY a LIMIT 1) as test 
 FROM t1 as tt GROUP BY tt.a;
*/

# ERROR 138 (HY000) at line 46: IDB-2027: Non supported item in aggregate function test.
/*
 SELECT tt.a, MAX(
 (SELECT (SELECT t.c FROM t1 AS t WHERE t1.a=t.a AND t.d=MAX(t1.b + tt.a)
   LIMIT 1)
  FROM t1 WHERE t1.a=tt.a GROUP BY a LIMIT 1)) as test 
  FROM t1 as tt GROUP BY tt.a;
*/

DROP TABLE t1;


CREATE TABLE t1 (a int, b int) engine=infinidb; 
INSERT INTO t1 VALUES (1,1),(2,1);
SELECT 1 FROM t1 WHERE a = (SELECT COUNT(*) FROM t1);
DROP TABLE t1;

CREATE TABLE t1 (id int, st CHAR(2)) engine=infinidb;
INSERT INTO t1 VALUES (3,'FL'), (2,'GA'), (4,'FL'), (1,'GA'), (5,'NY'), (7,'FL'), (6,'NY');
  
CREATE TABLE t2 (id int) engine=infinidb;
INSERT INTO t2 VALUES (7), (5), (1), (3);

SELECT id, st FROM t1 
  WHERE st IN ('GA','FL') AND EXISTS(SELECT 1 FROM t2 WHERE t2.id=t1.id);
SELECT id, count(st) FROM t1 
  WHERE st IN ('GA','FL') AND EXISTS(SELECT 1 FROM t2 WHERE t2.id=t1.id)
    GROUP BY id order by id;

SELECT id, st FROM t1 
  WHERE st IN ('GA','FL') AND NOT EXISTS(SELECT 1 FROM t2 WHERE t2.id=t1.id);
SELECT id, count(st) FROM t1 
  WHERE st IN ('GA','FL') AND NOT EXISTS(SELECT 1 FROM t2 WHERE t2.id=t1.id)
    GROUP BY id order by id;

DROP TABLE t1;
drop table t2;


CREATE TABLE t1 (a int) engine=infinidb;
INSERT INTO t1 VALUES (1), (2);
SELECT * FROM (SELECT count(*) FROM t1) as res;
DROP TABLE t1;
 
CREATE TABLE t1 (a INTEGER, b INTEGER);
CREATE TABLE t2 (x INTEGER);
INSERT INTO t1 VALUES (1,11), (2,22), (2,22);
INSERT INTO t2 VALUES (1), (2);

SELECT (SELECT SUM(t1.a)/AVG(t2.x) FROM t2) FROM t1;
DROP TABLE t1;
drop table t2;

CREATE TABLE t1 (a INT, b INT) engine=infinidb;
INSERT INTO t1 VALUES (1, 2), (1,3), (1,4), (2,1), (2,2);

SELECT a1.a, COUNT(*) FROM t1 a1 WHERE a1.a = 1
AND EXISTS( SELECT a2.a FROM t1 a2 WHERE a2.a = a1.a)
GROUP BY a1.a order by 1;
DROP TABLE t1;

CREATE TABLE t1 (a INT);
CREATE TABLE t2 (a INT);
INSERT INTO t1 VALUES (1),(2);
INSERT INTO t2 VALUES (1),(2);
SELECT (SELECT SUM(t1.a) FROM t2 WHERE a=0) FROM t1;
SELECT (SELECT SUM(t1.a) FROM t2 WHERE a=1) FROM t1;
DROP TABLE t1;
drop table t2;

CREATE TABLE t1 (a1 INT, a2 INT) engine=infinidb;
CREATE TABLE t2 (b1 INT, b2 INT) engine=infinidb;

INSERT INTO t1 VALUES (100, 200);
INSERT INTO t1 VALUES (101, 201);
INSERT INTO t2 VALUES (101, 201);
INSERT INTO t2 VALUES (103, 203);

# ERROR 138 (HY000) at line 120: IDB-1001: Function 'isnull' can only be used in the outermost select or order by clause and cannot be used in conjunction with an aggregate function.
# SELECT ((a1,a2) IN (SELECT * FROM t2 WHERE b2 > 0)) IS NULL FROM t1;
DROP TABLE t1;
drop table t2;


CREATE TABLE t1 (a CHAR(1), b VARCHAR(10));
INSERT INTO t1 VALUES ('a', 'aa');
INSERT INTO t1 VALUES ('a', 'aaa');
SELECT a,b FROM t1 WHERE b IN (SELECT a FROM t1);
CREATE INDEX I1 ON t1 (a);
CREATE INDEX I2 ON t1 (b);
EXPLAIN SELECT a,b FROM t1 WHERE b IN (SELECT a FROM t1);
SELECT a,b FROM t1 WHERE b IN (SELECT a FROM t1);

CREATE TABLE t2 (a VARCHAR(1), b VARCHAR(10)) engine=infinidb;
# ERROR 138 (HY000) at line 149: IDB-3019: Subquery in INSERT statements is currently not supported.
# INSERT INTO t2 SELECT * FROM t1;
SELECT a,b FROM t2 WHERE b IN (SELECT a FROM t2);
SELECT a,b FROM t1 WHERE b IN (SELECT a FROM t1 WHERE LENGTH(a)<500);

DROP TABLE t1;
drop table t2;


CREATE TABLE t1(a INT, b INT);
INSERT INTO t1 VALUES (1,1), (1,2), (2,3), (2,4);

SELECT a AS out_a, MIN(b) FROM t1 t1_outer
WHERE b > (SELECT MIN(b) FROM t1 WHERE a = t1_outer.a)
GROUP BY a order by 1;

DROP TABLE t1;


CREATE TABLE t1 (a INT) engine=infinidb;
CREATE TABLE t2 (a INT) engine=infinidb;

INSERT INTO t1 VALUES (1),(2);
INSERT INTO t2 VALUES (1),(2);

SELECT 2 FROM t1 WHERE EXISTS ((SELECT 1 FROM t2 WHERE t1.a=t2.a));
# ERROR 138 (HY000) at line 153: IDB-3013: Subquery with union is currently only supported in the FROM clause.
# SELECT 2 FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t1.a=t2.a UNION SELECT 1 FROM t2 WHERE t1.a = t2.a);

DROP TABLE t1;
drop table t2;

