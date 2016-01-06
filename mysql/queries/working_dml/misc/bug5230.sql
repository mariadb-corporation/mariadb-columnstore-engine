drop table if exists region1;
create table  region1
   (
    r_regionkey int,
    r_name char(25),
    r_comment varchar(152)
   ) engine=infinidb;
start transaction;
insert into region1 select * from tpch1.region;
rollback;
select * from region1;
start transaction;
insert into region1 select * from tpch1.region;
commit;
select * from region1;
drop table region1;

