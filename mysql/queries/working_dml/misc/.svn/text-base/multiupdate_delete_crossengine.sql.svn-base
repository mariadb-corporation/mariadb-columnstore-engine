alter table lineitem add column l_shippriority int;
create table if not exists orders_myisam as select * from orders;

update lineitem join orders_myisam on l_orderkey = o_orderkey set l_shippriority = o_shippriority where o_orderkey 
<= 500000;
select count(*) as count1 from lineitem join orders_myisam on l_orderkey = o_orderkey where l_shippriority <> 
o_shippriority and o_orderkey <= 500000;

update orders_myisam join lineitem on o_orderkey = l_orderkey set l_shippriority = 999 where o_orderkey <= 500000;
select count(*) as count2 from lineitem where l_shippriority <> 999 and l_orderkey <= 500000;

alter table lineitem drop column l_shippriority;

alter table customer add column c1 int;

update customer join orders_myisam on c_custkey = o_custkey set c1=9999 where o_orderkey <= 500000;
select count(*) count3 from customer where c1 is null;
select count(*) count4 from customer where c1 is not null;
select count(*) count5 from customer join orders_myisam on c_custkey = o_custkey where c1 <> 9999 and o_orderkey <= 
500000;

alter table customer drop column c1;
drop table orders_myisam;

alter table orders add column c1 int;
create table if not exists customer_myisam as select * from customer;
create table if not exists part_myisam as select * from part;

update orders join customer_myisam on o_custkey = c_custkey join lineitem on o_orderkey = l_orderkey join part on 
l_partkey = p_partkey set orders.c1=l_linenumber where l_linenumber = 1 and l_orderkey <= 500000;

select count(*) count1 from orders where c1 <> 1 and o_orderkey <= 500000;

update part_myisam join lineitem on p_partkey = l_partkey join orders on l_orderkey = o_orderkey join customer on 
o_custkey = c_custkey set orders.c1=99 where l_linenumber = 1 and l_orderkey <= 400000;

select count(*) count2 from orders where c1 = 99;

alter table orders drop column c1;
drop table part_myisam;
drop table customer_myisam;

create table if not exists multidelete (c1 int)engine=infinidb;
create table if not exists multidelete_myisam (c2 int);
insert into multidelete values (1),(2),(3);
insert into multidelete_myisam values (2),(3),(4);
update multidelete set c1 = 2 where c1 in (select c2 from multidelete_myisam);
select * from multidelete;
delete from multidelete where c1 in (select c2 from multidelete_myisam);
select * from multidelete;
drop table multidelete;
drop table multidelete_myisam;

