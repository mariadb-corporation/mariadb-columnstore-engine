drop table if exists t1;
create table t1 (a float)engine=infinidb;
insert into t1 values (1.5),(2.5),(3.5);
# ERROR 138 (HY000) at line 24: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 1.5 IN (SELECT * from t1);
# ERROR 138 (HY000) at line 24: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 10.5 IN (SELECT * from t1);
# ERROR 138 (HY000) at line 24: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select NULL IN (SELECT * from t1);
update t1 set a=NULL where a=2.5;
# ERROR 138 (HY000) at line 24: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 1.5 IN (SELECT * from t1);
# ERROR 138 (HY000) at line 24: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 3.5 IN (SELECT * from t1);
# ERROR 138 (HY000) at line 24: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 10.5 IN (SELECT * from t1);
# ERROR 138 (HY000) at line 24: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 1.5 > ALL (SELECT * from t1);
# ERROR 138 (HY000) at line 24: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 10.5 > ALL (SELECT * from t1);
# ERROR 138 (HY000) at line 24: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 1.5 > ANY (SELECT * from t1);
# ERROR 138 (HY000) at line 24: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 10.5 > ANY (SELECT * from t1);
select (select a+1) from t1;
drop table t1;


