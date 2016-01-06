drop table if exists t1;
create table t1 (a int) engine=infinidb;
insert into t1 values (1),(2),(3);
# ERROR 138 (HY000) at line 4: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 1 IN (SELECT * from t1);
# ERROR 138 (HY000) at line 5: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 10 IN (SELECT * from t1);
# ERROR 138 (HY000) at line 22: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select NULL IN (SELECT * from t1);
update t1 set a=NULL where a=2;
# ERROR 138 (HY000) at line 9: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 1 IN (SELECT * from t1);
# ERROR 138 (HY000) at line 10: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 3 IN (SELECT * from t1);
# ERROR 138 (HY000) at line 11: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 10 IN (SELECT * from t1);
# ERROR 138 (HY000) at line 22: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 1 > ALL (SELECT * from t1);
# ERROR 138 (HY000) at line 22: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 10 > ALL (SELECT * from t1);
# ERROR 138 (HY000) at line 22: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 1 > ANY (SELECT * from t1);
# ERROR 138 (HY000) at line 6: IDB-3011: Non-supported item in Order By clause.
# select 10 > ANY (SELECT * from t1);
drop table t1;

