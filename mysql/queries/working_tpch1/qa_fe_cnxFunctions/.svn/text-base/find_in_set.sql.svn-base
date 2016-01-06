drop table if exists tfind_in_set;
create table tfind_in_set(c1 char(25), c2 varchar(255), c3 decimal(4,2)) engine=infinidb;
insert into tfind_in_set values('name', 'My,name,is Tod', 2), ('a,b', 'a,b,c', 1), ('a name', 'what is a name, a name is,a name', 3.2);
select c1, c2, find_in_set(c1,c2) from tfind_in_set;
select c1, c2 from tfind_in_set where find_in_set(c1,c2)=3.0;
select c1, c2 from tfind_in_set where find_in_set(c1,c2)=c3;
drop table tfind_in_set;

