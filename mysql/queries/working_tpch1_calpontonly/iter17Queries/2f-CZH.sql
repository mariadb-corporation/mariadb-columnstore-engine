select
l_orderkey,
l_quantity,
l_extendedprice
from
customer,
orders,
lineitem
where
	c_custkey < 6543
	and l_partkey > 99999
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
	c_custkey < 62333
	and l_partkey > 99999
	and c_nationkey > 10
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
	c_custkey < 116
	and o_orderdate > "1996-01-01"
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
	c_custkey < 643
	and o_orderdate between "1996-01-01" and "1997-01-01"
	and l_receiptdate < "1997-01-01"
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
	c_custkey < 234
	and o_orderdate between "1996-01-01" and "1997-01-01"
	and l_quantity < 130.0
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
	c_custkey < 5022
	and o_orderdate between "1996-01-01" and "1997-01-01"
	and l_shipmode = "TRUCK"
	and l_quantity between 10.0 and 320.0
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey;
