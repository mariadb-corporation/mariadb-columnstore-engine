drop table if exists test;
drop view if exists test_v;
create table test (test1 numeric,test2 varchar(20)) engine=infinidb; 
create view test_v as select * from test;
insert into test values (1,'data1');
insert into test values (2,'data2');
insert into test values (3,'data3');

select T.test1,T.test2 from test_v T;
select T.test1,T.test2 from test_v t;
select T.test1 from (select * from test_v)T;
drop table test;
drop view test_v;
