select
l_orderkey,
l_quantity,
l_extendedprice
from
customer,
orders,
lineitem,
part,
partsupp
where
	c_custkey < 6
	and o_orderdate between "1992-01-01" and "1996-10-01"
	and l_receiptdate < "1999-05-01"
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey
	and p_partkey = l_partkey
	and ps_partkey = l_partkey;

select
l_orderkey,
l_quantity,
l_extendedprice
from
customer,
orders,
lineitem,
part,
supplier,
nation
where
	c_custkey < 6
	and n_nationkey < 15
	and s_suppkey < 127
	and o_orderdate between "1944-01-01" and "1996-10-01"
	and l_receiptdate < "1996-05-01"
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey
	and p_partkey = l_partkey
	and n_nationkey = s_nationkey
	and c_nationkey = n_nationkey;

select
l_orderkey,
l_quantity,
l_extendedprice
from
customer,
orders,
lineitem,
part,
partsupp
where
	c_custkey < 6
	and ps_availqty < 19991
	and o_orderdate between "1996-01-01" and "1999-10-01"
	and l_receiptdate < "1996-05-01"
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey
	and p_partkey = l_partkey
	and ps_partkey = l_partkey
;
