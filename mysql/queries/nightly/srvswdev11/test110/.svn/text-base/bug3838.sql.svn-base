drop table if exists tbug3838;
create table tbug3838(c1 int) engine=infinidb;
insert into tbug3838 values(1),(2),(3),(4);
select * from tbug3838;
select calshowpartitions('tbug3838', 'c1') into @x;
select @x;
select substr(@x, 80, 5) into @y;
select @y;
select caldroppartitions('tbug3838', @y);
select * from tbug3838;

