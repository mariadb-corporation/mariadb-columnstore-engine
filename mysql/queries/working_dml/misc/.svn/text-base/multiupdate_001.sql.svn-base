alter table lineitem add column l_shippriority int;

update lineitem join orders on l_orderkey = o_orderkey set l_shippriority = o_shippriority where o_orderkey <= 500000;
select count(*) as count1 from lineitem join orders on l_orderkey = o_orderkey where l_shippriority <> o_shippriority and o_orderkey <= 500000;

update orders join lineitem on o_orderkey = l_orderkey set l_shippriority = 999 where o_orderkey <= 500000;
select count(*) as count2 from lineitem where l_shippriority <> 999 and l_orderkey <= 500000;

alter table lineitem drop column l_shippriority;

alter table customer add column c1 int;

update customer join orders on c_custkey = o_custkey set c1=9999 where o_orderkey <= 500000;
select count(*) count3 from customer where c1 is null;
select count(*) count4 from customer where c1 is not null;
select count(*) count5 from customer join orders on c_custkey = o_custkey where c1 <> 9999 and o_orderkey <= 500000;

alter table customer drop column c1;
