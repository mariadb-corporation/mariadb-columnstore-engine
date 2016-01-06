select
l_orderkey,
l_quantity,
l_extendedprice
from
customer,
orders,
lineitem
where
	c_custkey < 4400
	and l_orderkey > 5999000
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey;

select
l_orderkey,
l_quantity,
l_extendedprice
from
customer,
orders,
lineitem
where
	c_custkey < 10
	and o_orderkey > 5000000
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey;

select
l_orderkey,
l_quantity,
l_extendedprice
from
customer,
orders,
lineitem
where
	c_custkey < 10
	and o_orderkey > 5000000
	and l_orderkey > 10
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey;
