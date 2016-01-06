drop table if exists t1;
drop table if exists t2;
drop table if exists t3;

create table t1( f1 int,f2 int) engine=infinidb;
insert into t1 values (1,1),(2,2);
# ERROR doesn't compare.
# select tt.t from (select 'crash1' as t, f2 from t1) as tt left join t1 on tt.t = 'crash2' and tt.f2 = t1.f2 where tt.t = 'crash1';
drop table t1;


create table t1 (c int) engine=infinidb;                              
insert into t1 values (1142477582), (1142455969);
create table t2 (a int, b int) engine=infinidb;
insert into t2 values (2, 1), (1, 0);
# ERROR 122 (HY000) at line 17: IDB-3017: Subquery in DELETE statements is currently not supported.
# delete from t1 where c <= 1140006215 and (select b from t2 where a = 2) = 1;
drop table t1;
drop table t2;


create table t1 (i int, j bigint) engine=infinidb;
insert into t1 values (1, 2), (2, 2), (3, 2);
select * from (select min(i) from t1 where j=(select * from (select min(j) from t1) t2)) t3;
drop table t1;

# WWW.  Removed unsigned below.
# CREATE TABLE t1 (i BIGINT UNSIGNED) engine=infinidb;
CREATE TABLE t1 (i BIGINT) engine=infinidb;
INSERT INTO t1 VALUES (10000000000000000000); 
INSERT INTO t1 VALUES (1);

# WWW.  Removed unsigned below.
# CREATE TABLE t2 (i BIGINT UNSIGNED) engine=infinidb;
CREATE TABLE t2 (i BIGINT) engine=infinidb;
INSERT INTO t2 VALUES (10000000000000000000);
INSERT INTO t2 VALUES (1);

SELECT t1.i FROM t1 JOIN t2 ON t1.i = t2.i;
SELECT t1.i FROM t1 WHERE t1.i = (SELECT MAX(i) FROM t2);

DROP TABLE t1;
DROP TABLE t2;


CREATE TABLE t1 (
  id bigint(20),
  name varchar(255)) engine=infinidb;
INSERT INTO t1 VALUES
  (1, 'Balazs'), (2, 'Joe'), (3, 'Frank');

# WWW.  Changed column named date to xdate below.
CREATE TABLE t2 (
  id bigint(20),
  mid bigint(20),
  xdate date) engine=infinidb;
INSERT INTO t2 VALUES 
  (1, 1, '2006-03-30'), (2, 2, '2006-04-06'), (3, 3, '2006-04-13'),
  (4, 2, '2006-04-20'), (5, 1, '2006-05-01');

# ERROR 138 (HY000) at line 62: IDB-3019: Limit within a correlated subquery is currently not supported.
/*
SELECT *,
      (SELECT xdate FROM t2 WHERE mid = t1.id
         ORDER BY xdate DESC LIMIT 0, 1) AS date_last,
      (SELECT xdate FROM t2 WHERE mid = t1.id
         ORDER BY xdate DESC LIMIT 3, 1) AS date_next_to_last
  FROM t1;
*/

# ERROR 138 (HY000) at line 66: IDB-2021: 'xdate' is not in GROUP BY clause.
# SELECT *,
#      (SELECT COUNT(*) FROM t2 WHERE mid = t1.id
#         ORDER BY xdate DESC LIMIT 1, 1) AS date_count
#  FROM t1;
# SELECT *,
#      (SELECT xdate FROM t2 WHERE mid = t1.id
#         ORDER BY xdate DESC LIMIT 0, 1) AS date_last,
#      (SELECT xdate FROM t2 WHERE mid = t1.id
#         ORDER BY xdate DESC LIMIT 1, 1) AS date_next_to_last
#   FROM t1;
DROP TABLE t1;
drop table t2;


CREATE TABLE t1 (id char(4), c int) engine=infinidb;
CREATE TABLE t2 (c int) engine=infinidb;

INSERT INTO t1 VALUES ('aa', 1);
INSERT INTO t2 VALUES (1);

# ERROR 138 (HY000) at line 86: IDB-3013: Subquery with union is currently only supported in the FROM clause.
# SELECT * FROM t1
#   WHERE EXISTS (SELECT c FROM t2 WHERE c=1
#                UNION
#                SELECT c from t2 WHERE c=t1.c);

INSERT INTO t1 VALUES ('bb', 2), ('cc', 3), ('dd',1);

# ERROR 138 (HY000) at line 93: IDB-3013: Subquery with union is currently only supported in the FROM clause.
# SELECT * FROM t1
#  WHERE EXISTS (SELECT c FROM t2 WHERE c=1
#                UNION
#                SELECT c from t2 WHERE c=t1.c);

INSERT INTO t2 VALUES (2);
CREATE TABLE t3 (c int) engine=infinidb;
INSERT INTO t3 VALUES (1);

# ERROR 138 (HY000) at line 102: IDB-3013: Subquery with union is currently only supported in the FROM clause.
# SELECT * FROM t1
#  WHERE EXISTS (SELECT t2.c FROM t2 JOIN t3 ON t2.c=t3.c WHERE t2.c=1
#                UNION
#                SELECT c from t2 WHERE c=t1.c);

DROP TABLE t1;
drop table t2;
drop table t3;

                                                       
CREATE TABLE t1 (a int, b INT, c CHAR(10)) engine=infinidb;                          
INSERT INTO t1 VALUES                                                         
  (1,1,'a'), (1,2,'b'), (1,3,'c'), (1,4,'d'), (1,5,'e'),                      
  (2,1,'f'), (2,2,'g'), (2,3,'h'), (3,4,'i'), (3,3,'j'),                      
  (3,2,'k'), (3,1,'l'), (1,9,'m');                                   
         
# ERROR 138 (HY000) at line 126: IDB-2026: Group function is not allowed in WHERE clause.
/*
SELECT a, MAX(b),                                                             
      (SELECT t.c FROM t1 AS t WHERE t1.a=t.a AND t.b=MAX(t1.b)) AS test      
  FROM t1 GROUP BY a;                                                         
*/
DROP TABLE t1;      

