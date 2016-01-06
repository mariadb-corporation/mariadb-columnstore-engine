drop table if exists t1_myisam;
create table t1_myisam (id int, c1 date);
select * from region left join (select * from t1_myisam)a on r_regionkey=id;
insert into t1_myisam values (1, '2009-12-31'), (2, '2009-12-28');
select count(*) from datatypetestm left join (select * from t1_myisam)a on cdate = c1;
drop table t1_myisam;

