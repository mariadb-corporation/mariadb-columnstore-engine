drop table if exists t1;
create table t1 (a varchar(20)) engine=infinidb;
insert into t1 values ('A'),('BC'),('DEF');
# ERROR 138 (HY000) at line 4: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 'A' IN (SELECT * from t1);
# ERROR 138 (HY000) at line 4: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 'XYZS' IN (SELECT * from t1);
# ERROR 138 (HY000) at line 24: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select NULL IN (SELECT * from t1);
update t1 set a=NULL where a='BC';
# ERROR 138 (HY000) at line 4: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 'A' IN (SELECT * from t1);
# ERROR 138 (HY000) at line 4: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 'DEF' IN (SELECT * from t1);
# ERROR 138 (HY000) at line 4: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 'XYZS' IN (SELECT * from t1);
# ERROR 138 (HY000) at line 24: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 'A' > ALL (SELECT * from t1);
# ERROR 138 (HY000) at line 24: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 'XYZS' > ALL (SELECT * from t1);
# ERROR 138 (HY000) at line 24: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 'A' > ANY (SELECT * from t1);
# ERROR 138 (HY000) at line 24: IDB-3016: Function or Operator with sub query on the SELECT clause is currently not supported.
# select 'XYZS' > ANY (SELECT * from t1);
drop table t1;
