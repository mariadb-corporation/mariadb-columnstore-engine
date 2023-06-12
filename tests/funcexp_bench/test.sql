use test;

drop table if exists t1;
create table t1 (a int, b int) engine=columnstore;

insert into t1 values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10);

select (a * 3 + b) from t1;
