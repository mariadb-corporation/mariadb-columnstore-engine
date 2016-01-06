drop table if exists t1;
drop table if exists t2;
drop table if exists t3;

CREATE TABLE t1(select_id BIGINT, values_id BIGINT) engine=infinidb;
INSERT INTO t1 VALUES (1, 1);
CREATE TABLE t2 (select_id BIGINT, values_id BIGINT) engine=infinidb;
INSERT INTO t2 VALUES (1,1), (0, 1), (0, 2), (0, 3), (1, 5);

SELECT values_id FROM t1 
WHERE values_id IN (SELECT values_id FROM t2
                    WHERE select_id IN (1, 0));
SELECT values_id FROM t1 
WHERE values_id IN (SELECT values_id FROM t2
                    WHERE select_id BETWEEN 0 AND 1);

SELECT values_id FROM t1 
WHERE values_id IN (SELECT values_id FROM t2
                   WHERE select_id = 0 OR select_id = 1);

DROP TABLE t1;
drop table t2;


CREATE TABLE t1 (a int, b int) engine=infinidb;
CREATE TABLE t2 (c int, d int) engine=infinidb;
CREATE TABLE t3 (e int) engine=infinidb;

INSERT INTO t1 VALUES 
  (1,10), (2,10), (1,20), (2,20), (3,20), (2,30), (4,40);
INSERT INTO t2 VALUES
  (2,10), (2,20), (4,10), (5,10), (3,20), (2,40);
INSERT INTO t3 VALUES (10), (30), (10), (20) ;

SELECT a, MAX(b), MIN(b) FROM t1 GROUP BY a order by 1;
SELECT * FROM t2;
SELECT * FROM t3;

# ERROR 138 (HY000) at line 41: IDB-3015: Correlated subquery in HAVING clause is currently not supported.
/*
SELECT a FROM t1 GROUP BY a
 HAVING a IN (SELECT c FROM t2 WHERE MAX(b)>20);
*/

# ERROR 138 (HY000) at line 45: IDB-3015: Correlated subquery in HAVING clause is currently not supported.
/*
SELECT a FROM t1 GROUP BY a
 HAVING a IN (SELECT c FROM t2 WHERE MAX(b)<d);
*/

# ERROR 138 (HY000) at line 49: IDB-3015: Correlated subquery in HAVING clause is currently not supported.
/*
SELECT a FROM t1 GROUP BY a
 HAVING a IN (SELECT c FROM t2 WHERE MAX(b)>d);
*/

# ERROR 138 (HY000) at line 53: IDB-3008: Aggregate function in EXISTS subquery is currently not supported.
/*
SELECT a FROM t1 GROUP BY a
 HAVING a IN (SELECT c FROM t2
                WHERE  EXISTS(SELECT e FROM t3 WHERE MAX(b)=e AND e <= d));
*/
# ERROR 138 (HY000) at line 57: IDB-3008: Aggregate function in EXISTS subquery is currently not supported.
/*
SELECT a FROM t1 GROUP BY a
  HAVING a IN (SELECT c FROM t2
                 WHERE  EXISTS(SELECT e FROM t3 WHERE MAX(b)=e AND e < d));
*/
# ERROR 138 (HY000) at line 61: IDB-3008: Aggregate function in EXISTS subquery is currently not supported.
/*
SELECT a FROM t1 GROUP BY a
  HAVING a IN (SELECT c FROM t2
                 WHERE MIN(b) < d AND 
                        EXISTS(SELECT e FROM t3 WHERE MAX(b)=e AND e <= d));
*/

SELECT a, SUM(a) FROM t1 GROUP BY a order by 1;

# ERROR 138 (HY000) at line 59: IDB-3008: Aggregate function in EXISTS subquery is currently not supported.
/*
SELECT a FROM t1
  WHERE EXISTS(SELECT c FROM t2 GROUP BY c HAVING SUM(a) = c) GROUP BY a;
/

# ERROR 138 (HY000) at line 61: IDB-3008: Aggregate function in EXISTS subquery is currently not supported.
/*
SELECT a FROM t1 GROUP BY a
  HAVING EXISTS(SELECT c FROM t2 GROUP BY c HAVING SUM(a) = c);
*/

# ERROR 138 (HY000) at line 77: IDB-3008: Aggregate function in EXISTS subquery is currently not supported.
/*
SELECT a FROM t1
   WHERE a < 3 AND
         EXISTS(SELECT c FROM t2 GROUP BY c HAVING SUM(a) != c) GROUP BY a;
*/
# ERROR 138 (HY000) at line 81: IDB-3008: Aggregate function in EXISTS subquery is currently not supported.
/*
 SELECT a FROM t1
    WHERE a < 3 AND
         EXISTS(SELECT c FROM t2 GROUP BY c HAVING SUM(a) != c);
*/

# ERROR 138 (HY000) at line 86: IDB-3015: Correlated subquery in HAVING clause is currently not supported.
/*
 SELECT t1.a FROM t1 GROUP BY t1.a
  HAVING t1.a IN (SELECT t2.c FROM t2 GROUP BY t2.c
                    HAVING AVG(t2.c+SUM(t1.b)) > 20);
*/

# ERROR 138 (HY000) at line 91: IDB-3015: Correlated subquery in HAVING clause is currently not supported.
/*
 SELECT t1.a FROM t1 GROUP BY t1.a
   HAVING t1.a IN (SELECT t2.c FROM t2 GROUP BY t2.c
                     HAVING AVG(SUM(t1.b)) > 20);
*/

# ERROR 138 (HY000) at line 96: IDB-3015: Correlated subquery in HAVING clause is currently not supported.
/*
 SELECT t1.a, SUM(b) AS sum  FROM t1 GROUP BY t1.a
  HAVING t1.a IN (SELECT t2.c FROM t2 GROUP BY t2.c
                    HAVING t2.c+sum > 20);
*/

DROP TABLE t1;
drop table t2;
drop table t3;

