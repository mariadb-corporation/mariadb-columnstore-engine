drop table if exists bug5328;
create table bug5328 (c1 int) engine=infinidb;
insert into bug5328 values (-1);
select * from bug5328;
select caldroppartitionsbyvalue('bug5328','c1','-1','-1');
drop table if exists bug5328;
