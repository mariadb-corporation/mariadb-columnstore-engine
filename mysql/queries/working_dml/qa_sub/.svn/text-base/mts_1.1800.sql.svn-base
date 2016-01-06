drop table if exists t1;
drop table if exists t2;
drop table if exists t3;

create table t1 (df decimal(5,1)) engine=infinidb;
insert into t1 values(1.1);
# ERROR 138 (HY000) at line 7: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 1.1 * exists(select * from t1);
drop table t1;


CREATE table t1 ( c1 integer ) engine=infinidb;
INSERT INTO t1 VALUES ( 1 );
INSERT INTO t1 VALUES ( 2 );
INSERT INTO t1 VALUES ( 3 );

CREATE TABLE t2 ( c2 integer ) engine=infinidb;
INSERT INTO t2 VALUES ( 1 );
INSERT INTO t2 VALUES ( 4 );
INSERT INTO t2 VALUES ( 5 );

SELECT * FROM t1 LEFT JOIN t2 ON c1 = c2 WHERE c2 IN (1);

SELECT * FROM t1 LEFT JOIN t2 ON c1 = c2
  WHERE c2 IN ( SELECT c2 FROM t2 WHERE c2 IN ( 1 ) );

DROP TABLE t1;
drop table t2;

CREATE TABLE t1 ( c1 integer ) engine=infinidb;
INSERT INTO t1 VALUES ( 1 );
INSERT INTO t1 VALUES ( 2 );
INSERT INTO t1 VALUES ( 3 );
INSERT INTO t1 VALUES ( 6 ); 
 
CREATE TABLE t2 ( c2 integer ) engine=infinidb;
INSERT INTO t2 VALUES ( 1 );
INSERT INTO t2 VALUES ( 4 );
INSERT INTO t2 VALUES ( 5 );
INSERT INTO t2 VALUES ( 6 );

CREATE TABLE t3 ( c3 integer ) engine=infinidb;
INSERT INTO t3 VALUES ( 7 );
INSERT INTO t3 VALUES ( 8 );

# ERROR 138 (HY000) at line 47: IDB-3018: t2 table is not joined in the subquery.
# SELECT c1,c2 FROM t1 LEFT JOIN t2 ON c1 = c2 
# WHERE EXISTS (SELECT c3 FROM t3 WHERE c2 IS NULL );

DROP TABLE t1;
drop table t2;
drop table t3;

CREATE TABLE t1 (EMPNUM   CHAR(3)) engine=infinidb;
CREATE TABLE t2 (EMPNUM   CHAR(3)) engine=infinidb;
INSERT INTO t1 VALUES ('E1'),('E2');
INSERT INTO t2 VALUES ('E1');
# ERROR 122 (HY000) at line 59: IDB-3017: Subquery in DELETE statements is currently not supported.
# DELETE FROM t1
# WHERE t1.EMPNUM NOT IN
#      (SELECT t2.EMPNUM
#       FROM t2
#       WHERE t1.EMPNUM = t2.EMPNUM);
select * from t1;
DROP TABLE t1;
drop table t2;

