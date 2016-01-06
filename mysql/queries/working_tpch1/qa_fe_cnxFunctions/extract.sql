create table if not exists bug4678 (c1 varchar(30), c2 varchar(10), c3 int, c4 bigint)engine=infinidb;
insert into bug4678 values ('1999-10-11 10:22:15', '2000-11-05', 20001105, 19991011102215);
select extract(hour from c1), extract(year from c2), extract(month from c3), extract(minute from c4) from bug4678;
drop table bug4678;
