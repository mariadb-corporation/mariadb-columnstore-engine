select c_name, n_name as country, r_name, l_orderkey,l_quantity,l_extendedprice as total 
from customer,orders,lineitem,nation,region 
where n_regionkey = r_regionkey and c_custkey = o_custkey and o_orderkey = l_orderkey and 
n_nationkey = c_nationkey and l_orderkey < 10000 and l_quantity =10 and l_discount = 0.03 and 
o_orderstatus in ( 'F', 'O', 'P') and o_orderdate > '1995-01-01';

select c_name, n_name as country, r_name, l_orderkey,l_quantity,l_extendedprice as total 
from customer,orders,lineitem,nation,region 
where n_regionkey = r_regionkey and c_custkey = o_custkey and o_orderkey = l_orderkey and n_nationkey = c_nationkey and 
l_orderkey < 10200 and l_quantity =10 and l_discount = 3;

select c_name, n_name as country, r_name, l_orderkey,l_quantity,l_extendedprice as total 
from customer,orders,lineitem,nation,region 
where n_regionkey = r_regionkey and c_custkey = o_custkey and o_orderkey = l_orderkey and n_nationkey = c_nationkey and l_orderkey < 4000 and l_quantity =10 and l_discount = 0.03 and o_orderstatus in ( 'F', 'O', 'P');

