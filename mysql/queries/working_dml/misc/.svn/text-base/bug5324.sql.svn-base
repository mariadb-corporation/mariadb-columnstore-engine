drop table if exists bug5324;
create table bug5324 (c1 int, c2 varchar(20)) engine=infinidb;
insert into bug5324 values (1,'x');
select * from bug5324;
select caldroppartitionsbyvalue('bug5324','c1','1','1');
insert into bug5324 values (2,'y');
select * from bug5324;
drop table if exists bug5324;
