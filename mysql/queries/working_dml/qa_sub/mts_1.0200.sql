# WWW - Added order bys to a few queries.
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
drop table if exists t4;
drop table if exists t5;
drop table if exists t6;
drop table if exists t7;
drop table if exists t8;

create table t1 (a int) engine=infinidb;
create table t2 (a int, b int) engine=infinidb;
create table t3 (a int) engine=infinidb;
create table t4 (a int, b int) engine=infinidb;
insert into t1 values (2);
insert into t2 values (1,7),(2,7);
insert into t4 values (4,8),(3,8),(5,9);

select (select a from t1 where t1.a = 2) as a2, (select b from t2 where t2.b=1) as a1;
select (select a from t1 where t1.a=t2.a) as a1, a as a2 from t2;
select (select a from t1 where t1.a=t2.b) a1, a as a2 from t2;
select (select a from t1) as a1, a as a2, (select 1) from t2;
select (select a from t3), a from t2 order by 1, 2;
select * from t2 where t2.a=(select a from t1) order by 1, 2;

insert into t3 values (6),(7),(3);
select * from t2 where t2.b=(select a from t3 where a=7) order by 1, 2;
(select * from t2 where t2.b=(select a from t3 where a=7)) union (select * from t4 order by a limit 2) order by 1, 2 limit 3;

# WWW - Added order by 1, 2 below.
(select * from t2 where t2.b=(select a from t3 where a=7)) union (select * from t4 where t4.b=(select max(t2.a)*4 from t2) order by a, b) order by 1, 2;

# ERROR 138 (HY000) at line 21: IDB-1000: 't2' is not joined.
# select (select a from t3 where a<t2.a*4 order by 1 desc limit 1) as a1, a as a2 from t2;
select (select t3.a from t3 where a<8 order by 1 desc limit 1) as a1, tt.a as a2 from (select * from t2 where a>1) as tt;

select * from t1 where t1.a=(select t2.a from t2 where t2.b=(select max(a) from t3) order by 1 desc limit 1);
# ERROR 138 (HY000) at line 25: IDB-3012: Scalar filter and semi join are not from the same pair of tables.
# select * from t1 where t1.a=(select t2.a from t2 where t2.b=(select max(a) from t3 where t3.a > t1.a) order by 1 desc limit 1);
# ERROR 138 (HY000) at line 26: IDB-3012: Scalar filter and semi join are not from the same pair of tables.
# select * from t1 where t1.a=(select t2.a from t2 where t2.b=(select max(a) from t3 where t3.a < t1.a) order by 1 desc limit 1);
# WWW.  Added x as alias for column below.
# ERROR 138 (HY000) at line 42: IDB-2027: Non supported item in aggregate function avg(t2.a+(select min(t3.a) from t3 where t3.a >= t4.a)).
# select b,(select avg(t2.a+(select min(t3.a) from t3 where t3.a >= t4.a)) from t2) x from t4;

select * from t3 where exists (select * from t2 where t2.b=t3.a);
select * from t3 where not exists (select * from t2 where t2.b=t3.a);
select * from t3 where a in (select b from t2) order by 1;
select * from t3 where a not in (select b from t2) order by 1;

insert into t4 values (12,7),(1,7),(10,9),(9,6),(7,6),(3,9),(1,10);

# ERROR 138 (HY000): IDB-3015: Correlated subquery in HAVING clause is currently not supported.
# select b,max(a) as ma from t4 group by b having b < (select max(t2.a) from t2 where t2.b=t4.b);
insert into t2 values (2,10);
# ERROR 138 (HY000): IDB-3015: Correlated subquery in HAVING clause is currently not supported.
# select b,max(a) as ma from t4 group by b having ma < (select max(t2.a) from t2 where t2.b=t4.b);
delete from t2 where a=2 and b=10;

# ERROR 138 (HY000): IDB-3015: Correlated subquery in HAVING clause is currently not supported.
# select b,max(a) as ma from t4 group by b having b >= (select max(t2.a) from t2 where t2.b=t4.b);

create table t5 (a int) engine=infinidb;
# ERROR 138 (HY000) at line 43: IDB-3013: Subquery with union is currently only supported in the FROM clause.
# select (select a from t1 where t1.a=t2.a union select a from t5 where t5.a=t2.a) as a1, a as a2 from t2;
insert into t5 values (5);
# ERROR 138 (HY000) at line 45: IDB-3013: Subquery with union is currently only supported in the FROM clause.
# select (select a from t1 where t1.a=t2.a union select a from t5 where t5.a=t2.a) as a1, a as a2 from t2;
insert into t5 values (2);
# ERROR 138 (HY000): IDB-3013: Subquery with union is currently only supported in the FROM clause.
# select (select a from t1 where t1.a=t2.a union select a from t5 where t5.a=t2.a) alias, a from t2;

create table t6 (patient_uq int, clinic_uq int) engine=infinidb;
# WWW.  Removed pk below.  create table t7( uq int primary key, name char(25)) engine=infinidb;
create table t7( uq int, name char(25)) engine=infinidb;
insert into t7 values(1,"Oblastnaia bolnitsa"),(2,"Bolnitsa Krasnogo Kresta");
insert into t6 values (1,1),(1,2),(2,2),(1,3);
select * from t6 where exists (select * from t7 where uq = clinic_uq);


select * from t1 where a= (select t2.a from t2,t4 where t2.b=t4.b limit 1);

drop table t1;
drop table t2;
drop table t3;

CREATE TABLE t3 (a varchar(20),b char(1)) engine=infinidb;
INSERT INTO t3 VALUES ('W','a'),('A','c'),('J','b');
CREATE TABLE t2 (a varchar(20),b int) engine=infinidb;
INSERT INTO t2 VALUES ('W','1'),('A','3'),('J','2');
CREATE TABLE t1 (a varchar(20),b date) engine=infinidb;
INSERT INTO t1 VALUES ('W','1732-02-22'),('A','1735-10-30'),('J','1743-04-13');
SELECT * FROM t1 WHERE b = (SELECT MIN(b) FROM t1);
SELECT * FROM t2 WHERE b = (SELECT MIN(b) FROM t2);
SELECT * FROM t3 WHERE b = (SELECT MIN(b) FROM t3);

CREATE TABLE `t8` (
  `pseudo` varchar(35), `email` varchar(60)) ENGINE=infinidb;

INSERT INTO t8 (pseudo,email) VALUES ('joce','test');
INSERT INTO t8 (pseudo,email) VALUES ('joce1','test1');
INSERT INTO t8 (pseudo,email) VALUES ('2joce1','2test1');
SELECT pseudo FROM t8 WHERE pseudo=(SELECT pseudo FROM t8 WHERE pseudo='joce');
SELECT pseudo FROM t8 WHERE pseudo in (SELECT pseudo FROM t8 WHERE pseudo LIKE '%joce%');

drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
drop table if exists t4;
drop table if exists t5;
drop table if exists t6;
drop table if exists t7;
drop table if exists t8;

