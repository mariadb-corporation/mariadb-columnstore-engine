drop table if exists bug5222;
create table bug5222 (col1 date, col2 datetime) engine=infinidb;
insert into bug5222 values('10000-01-02', '10000-01-02 12:10:14 1000000');
select * from bug5222;
update bug5222 set col1 = '10000-01-02';
select * from bug5222;
update bug5222 set col2 = '10000-01-02 12:10:14 1000000';
select * from bug5222;
drop table if exists bug5222;
