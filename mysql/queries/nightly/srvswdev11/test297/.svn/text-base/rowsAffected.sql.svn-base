drop table if exists a;
create table a (a char(20)) engine=infinidb;
insert into a values ('x');
select * from a;
update a join a y on a.a = y.a set a.a='99';
select * from a;
delete from a where a in (select x.a from (select a from a) x);
select * from a;
drop table a;