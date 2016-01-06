drop table if exists bug4391;
create table bug4391(c1 int comment 'autoincrement', c2 int) engine=infinidb;
insert into bug4391 values (10, 10);
update bug4391 set c1=4, c2=4;
insert into bug4391 (c2) values (1);
insert into bug4391 (c2) values (2),(3);
select * from bug4391;

