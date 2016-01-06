-- 2) Filter
--   b) Remove filters from orders, lineitem, customer (5)

-- 1) 
select
l_orderkey,
l_linenumber,
l_quantity,
o_orderpriority
from
customer,
orders,
lineitem
where   c_custkey < 22343
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
        and c_nationkey < 20
	and c_mktsegment > 'A'
	and l_quantity < 50
	and 'O' = o_orderstatus
	and '1-URGENT' < o_orderpriority;

-- 2)  
select
l_orderkey,
l_linenumber,
l_quantity,
o_orderpriority
from
customer,
orders,
lineitem
where   c_custkey < 2222
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
        and c_nationkey < 20
	and 'O' = o_orderstatus;

-- 3)  
select
l_orderkey,
l_linenumber,
l_quantity
from
customer,
orders,
lineitem
where   c_custkey < 90000
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
	and l_partkey < 20
	and l_quantity < 50
	and l_extendedprice between 20000 and 60000
	and l_tax < .08
	and o_orderdate > date'1997-01-01'
	and o_shippriority = 0
	and 9000 > c_acctbal
	and c_nationkey between 5 and 15;

-- 4)
select
l_orderkey,
l_linenumber,
l_quantity
c_custkey,
c_acctbal,
o_shippriority
from
customer,
orders,
lineitem
where   c_custkey < 9000
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
	and l_partkey < 20
	and l_quantity < 50
	and l_tax < .08
	and o_orderdate > date'1993-01-01'
	and 9000 > c_acctbal;

-- 5)

select
l_orderkey,
l_linenumber,
l_quantity
c_custkey,
c_acctbal,
o_shippriority
from
customer,
orders,
lineitem
where   c_custkey < 2876
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
	and l_tax < 99.08;
