drop table if exists t1;
drop table if exists t2;
drop table if exists t3;

create table t1 (a int, b int) engine=infinidb;
create table t2 (a int, b int) engine=infinidb;
create table t3 (a int, b int) engine=infinidb;
insert into t1 values (0,100),(1,2), (1,3), (2,2), (2,7), (2,-1), (3,10);
insert into t2 values (0,0), (1,1), (2,1), (3,1), (4,1);
insert into t3 values (3,3), (2,2), (1,1); 
# WWW - edits below to conform to IDB standards, aliased long table name and added join to t3.
# ERROR 138 (HY000) at line 13: IDB-3019: Limit within a correlated subquery is currently not supported.
# select a,(select count(distinct t1.b) as sum from t1,t2 where t1.a=t2.a and t2.b > 0 and t1.a <= t3.b group by t1.a order by sum limit 1) from t3;
# select a,(select count(distinct t1.b) as sum from t1,t2 where t1.a=t2.a and t2.b > 0 and t1.a <= t3.b and t3.b = t2.b group by t1.a order by sum limit 1) as colx from t3;
drop table t1;
drop table t2;
drop table t3;

create table t1 (s1 int) engine=infinidb;
create table t2 (s1 int) engine=infinidb;
insert into t1 values (1);
insert into t2 values (1);
# ERROR 138 (HY000) at line 20: IDB-3008: Aggregate function in EXISTS subquery is currently not supported.
# select * from t1 where exists (select s1 from t2 having max(t2.s1)=t1.s1);
drop table t1;
drop table t2;

create table t1(toid int,rd int) engine=infinidb;
create table t2(userid int,pmnew int,pmtotal int) engine=infinidb;
insert into t2 values(1,0,0),(2,0,0);
insert into t1 values(1,0),(1,0),(1,0),(1,12),(1,15),(1,123),(1,12312),(1,12312),(1,123),(2,0),(2,0),(2,1),(2,2);
select userid,pmtotal,pmnew, (select count(rd) from t1 where toid=t2.userid) calc_total, (select count(rd) from t1 where rd=0 and toid=t2.userid) calc_new from t2 where userid in (select distinct toid from t1);
drop table t1;
drop table t2;

create table t1 (s1 char(5)) engine=infinidb;
insert into t1 values ('tttt');
# ERROR 138 (HY000) at line 34: IDB-3013: Subquery with union is currently only supported in the FROM clause.
# select * from t1 where ('a','b')=(select 'a','b' from t1 union select 'a','b' from t1);
(select * from t1);
drop table t1;

create table t1 (s1 char(5)) engine=infinidb;
create table t2 (s1 char(5)) engine=infinidb;
insert into t1 values ('a1'),('a2'),('a3');
insert into t2 values ('a1'),('a2');
# ERROR 138 (HY000) at line 52: IDB-1001: Function 'not' can only be used in the outermost select or order by clause and cannot be used in conjunction with an aggregate function.
# select s1, s1 NOT IN (SELECT s1 FROM t2) from t1;
# ERROR 138 (HY000) at line 48: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select s1, s1 = ANY (SELECT s1 FROM t2) from t1;
# ERROR 138 (HY000) at line 52: IDB-1001: Function 'not' can only be used in the outermost select or order by clause and cannot be used in conjunction with an aggregate function.
# select s1, s1 <> ALL (SELECT s1 FROM t2) from t1;
# ERROR 138 (HY000) at line 52: IDB-1001: Function 'not' can only be used in the outermost select or order by clause and cannot be used in conjunction with an aggregate function.
# select s1, s1 NOT IN (SELECT s1 FROM t2 WHERE s1 < 'a2') from t1;
drop table t1;
drop table t2;


