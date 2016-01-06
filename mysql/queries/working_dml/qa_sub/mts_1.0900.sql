drop table if exists t1;
drop table if exists t2;
create table t1 (a int, b int, c varchar(10)) engine=infinidb;
create table t2 (a int) engine=infinidb;
insert into t1 values (1,2,'a'),(2,3,'b'),(3,4,'c');
insert into t2 values (1),(2),(NULL);
# ERROR 138 (HY000) at line 8: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select a, (select a,b,c from t1 where t1.a=t2.a) = ROW(a,2,'a'),(select c from t1 where a=t2.a)  from t2;
# ERROR 138 (HY000) at line 10: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select a, (select a,b,c from t1 where t1.a=t2.a) = ROW(a,3,'b'),(select c from t1 where a=t2.a) from t2;
# ERROR 138 (HY000) at line 12: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select a, (select a,b,c from t1 where t1.a=t2.a) = ROW(a,4,'c'),(select c from t1 where a=t2.a) from t2;
drop table t1;
drop table t2;

create table t1 (a int) engine=infinidb;
create table t2 (b int) engine=infinidb;
insert into t1 values (1),(2);
insert into t2 values (1);
select a from t1 where a in (select a from t1 where a in (select b from t2));
drop table t1;
drop table t2;

create table t1 (a int, b int) engine=infinidb;
create table t2 (a int, b int) engine=infinidb;
insert into t1 values (1,2),(1,3),(1,4),(1,5);
insert into t2 values (1,2),(1,3);
select * from t1 where row(a,b) in (select a,b from t2);
drop table t1;
drop table t2;


