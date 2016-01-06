select date('2010-02-01') into @xxx;
select @xxx;

drop table if exists bug3021;
create table bug3021(c1 date)engine=infinidb;
insert into bug3021 values ('2001-01-01');
update bug3021 set c1=@xxx;
select * from bug3021;
update bug3021 set c1=date('2010-11-14');
select * from bug3021;
drop table bug3021;
