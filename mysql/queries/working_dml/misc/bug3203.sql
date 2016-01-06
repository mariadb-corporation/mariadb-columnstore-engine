drop table if exists bug3203;
create table bug3203 (c1 varchar(20))engine=infinidb;
select * from bug3203 where c1 = 'x';
drop table if exists bug3203;
