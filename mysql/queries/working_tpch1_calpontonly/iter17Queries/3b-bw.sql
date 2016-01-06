-- 2) Filter
--   b) Remove filters from orders, lineitem, customer (5)

-- 1) 
select
l_orderkey,
l_linenumber,
l_quantity,
n_name,
r_name
from
customer,
orders,
lineitem,
region,
nation
where   c_custkey < 4442
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
        and c_nationkey < 20
	and c_nationkey = n_nationkey
	and r_regionkey = n_regionkey
	and c_mktsegment > 'A'
	and l_quantity < 50
	and 'O' = o_orderstatus
	and '1-URGENT' < o_orderpriority;

-- 2)  
select
l_orderkey,
l_linenumber,
l_quantity,
o_orderpriority,
n_comment,
r_comment
from
customer,
orders,
lineitem,
nation,
region
where   c_custkey < 1232
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
        and c_nationkey > 5
	and n_nationkey = c_nationkey
	and n_regionkey = r_regionkey
	and 'O' = o_orderstatus;

-- 3)  
select
l_orderkey,
l_linenumber,
l_quantity,
n_name,
r_name
from
customer,
orders,
lineitem,
nation,
region
where   c_custkey < 9000
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
	and l_partkey < 20
	and l_extendedprice between 20000 and 69000
	and l_tax < .08
	and o_orderdate > date'1993-01-01'
	and o_shippriority = 0
	and 9000 > c_acctbal
	and n_nationkey = c_nationkey
	and n_regionkey = r_regionkey
	and c_nationkey between 5 and 25;

