use test;

drop table if exists t1;
create table t1 (a tinyint, b tinyint) engine=columnstore;

insert into t1 values (1,1),(2,9),(3,8),(4,7),(5,6),(6,5),(7,4),(8,3),(9,2),(10,1);

select (a * 3 + b) from t1;