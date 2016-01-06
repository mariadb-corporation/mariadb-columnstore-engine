alter table orders add column c1 int;

update orders join customer on o_custkey = c_custkey join lineitem on o_orderkey = l_orderkey join part on l_partkey = p_partkey set orders.c1=l_linenumber where l_linenumber = 1 and l_orderkey <= 500000;

select count(*) count1 from orders where c1 <> 1 and o_orderkey <= 500000;

update part join lineitem on p_partkey = l_partkey join orders on l_orderkey = o_orderkey join customer on o_custkey = c_custkey set orders.c1=99 where l_linenumber = 1 and l_orderkey <= 400000;

select count(*) count2 from orders where c1 = 99;

alter table orders drop column c1;
