select c_name, n_name, l_orderkey,l_quantity,l_extendedprice  from customer,orders,lineitem,nation where c_custkey = o_custkey and o_orderkey = l_orderkey and n_nationkey = c_nationkey and n_nationkey = 1 and o_orderkey < 640;

select c_name, n_name as country, l_orderkey,l_quantity,l_extendedprice as total from customer,orders,lineitem,nation where c_custkey = o_custkey and o_orderkey = l_orderkey and n_nationkey = c_nationkey and l_orderkey < 1000 and l_quantity =10 and l_discount = 0.03 and o_orderstatus in ( 'F', 'O', 'P') and o_orderdate > '1995-01-01';

select c_name, n_name as country, l_orderkey,l_quantity,l_extendedprice from customer,orders,lineitem,nation where c_custkey = o_custkey and o_orderkey = l_orderkey and n_nationkey = c_nationkey and n_nationkey > 10 and o_orderkey in (64, 64);

select c_name, n_name as country, l_orderkey,l_quantity,l_extendedprice from customer,orders,lineitem,nation where c_custkey = o_custkey and o_orderkey = l_orderkey and n_nationkey = c_nationkey and n_nationkey > 20 and o_orderkey in ( 60,61, 62, 63 ,64 ,65 ,66,67,68 ,69);

select c_name, n_name as country, l_orderkey,l_quantity,l_extendedprice from customer,orders,lineitem,nation where c_custkey = o_custkey and o_orderkey = l_orderkey and n_nationkey = c_nationkey and n_name like 'ARG%' and (o_orderkey=62 or o_orderkey=63 or o_orderkey=64 or o_orderkey = 65 or o_orderkey = 66 or o_orderkey = 67 or o_orderkey = 68);

select c_name, n_name as country, l_orderkey,l_quantity,l_extendedprice from customer,orders,lineitem,nation where c_custkey = o_custkey and o_orderkey = l_orderkey and n_nationkey = c_nationkey and n_name like 'ARG%' and (o_orderkey=60 or o_orderkey=61 or o_orderkey=62 or o_orderkey = 63 or o_orderkey = 64 or l_orderkey = 65 or l_orderkey=66 or l_orderkey=67 or l_orderkey=68 or l_orderkey=69);

select c_name, n_name as country, l_orderkey,l_quantity,l_extendedprice
from customer,orders,lineitem,nation
where c_custkey = o_custkey and o_orderkey = l_orderkey and n_nationkey = c_nationkey
and n_name like 'ARG%'
-- Commented out below line until like implemented as function
-- and (n_name like 'ARG%' or n_name like 'ARGENT%' or n_name like 'ARGENTIN%')
-- Walt - commented out line below as it was mixing ands and ors against the nation table which we don't allow for iter 17.
-- and n_nationkey=1
and o_orderkey > 60 and o_orderkey < 70;

select c_name, n_name as country, l_orderkey,l_quantity,l_extendedprice as total
from customer,orders,lineitem,nation
where c_custkey = o_custkey and o_orderkey = l_orderkey and n_nationkey = c_nationkey
and n_name like 'ARG%'
-- Commented out below line until like implemented as function
-- and (n_name like 'ARG%' or n_name like 'ARGENT%' or n_name like 'ARGENTIN%')
-- Commented out line below as it was mixing ands and ors against same table.
-- and n_nationkey=1
and o_orderkey > 60 and o_orderkey < 70 and l_extendedprice < 10000;

select c_name, n_name as country, l_orderkey,l_quantity,l_extendedprice as total from customer,orders,lineitem,nation where c_custkey = o_custkey and o_orderkey = l_orderkey and n_nationkey = c_nationkey and l_orderkey < 1000 and l_quantity =10 and l_discount = 3;

select c_name, n_name as country, l_orderkey,l_quantity,l_extendedprice as total from customer,orders,lineitem,nation where c_custkey = o_custkey and o_orderkey = l_orderkey and n_nationkey = c_nationkey and l_orderkey < 1000 and l_quantity =10 and l_discount = 0.03 and o_orderstatus in ( 'F', 'O', 'P');

