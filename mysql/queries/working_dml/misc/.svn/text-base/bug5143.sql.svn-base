drop table if exists test_update_idb;
drop table if exists test_update2_idb;

create table test_update_idb (a int(11) default null, d date default null , s varchar(20) default null) engine=infinidb;
insert into test_update_idb values  (1, '2013-02-21', 'first');
insert into test_update_idb values  (2, '2013-02-21', 'second');
select * from test_update_idb;

create table test_update2_idb(a int, d date, s varchar(20)) engine=infinidb;
insert into test_update2_idb values  (0, '2010-01-05', NULL);
insert into test_update2_idb values  (0, '2010-03-05', NULL);
insert into test_update2_idb values  (1, '2010-02-05', NULL);
select * from test_update2_idb;

update test_update_idb set d = (select max(d) from test_update2_idb where test_update_idb.a = test_update2_idb.a) where a=1;
select * from test_update_idb;

select max(d) into @x from test_update2_idb; 
update test_update_idb set d=@x;
select * from test_update_idb;

insert into test_update2_idb values  (0, '2010-04-05', NULL);
update test_update_idb a set a.d = (select max(b.d) from test_update2_idb b) where a.a=1;
select * from test_update_idb;

drop table if exists test_update_idb;
drop table if exists test_update2_idb;
