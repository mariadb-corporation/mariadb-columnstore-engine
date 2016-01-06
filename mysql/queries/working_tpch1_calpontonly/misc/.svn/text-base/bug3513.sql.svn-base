drop table if exists daniel;
create table daniel (c1 int, c2 int) engine=infinidb;
insert into daniel values (1,10), (2,20), (3,30);
update daniel set c2=rand(c1)*100;
select * from daniel;
update daniel set c2=rand()*100;
select * from daniel;
update daniel set c2=rand()*100 where c1 > rand() + 30;
select * from daniel;
drop table daniel;



