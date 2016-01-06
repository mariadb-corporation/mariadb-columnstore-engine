drop table if exists unhex_test;
create table unhex_test (id int, val varchar(10))engine=infinidb;
insert into unhex_test values (1, '4D7953514C'),(2, '672a'), (3, '123 '), (4, null), (5, '178');
select id, unhex(val) from unhex_test order by 1;
drop table unhex_test;
