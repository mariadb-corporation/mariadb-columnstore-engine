-- 2) Filter
--   b) Add filters from orders (e.g. c_custkey < 100 and o_orderkey > 100) (5)


-- Base query.
select
l_orderkey,
l_linenumber,
l_quantity
from
customer,
orders,
lineitem
where   c_custkey < 6233
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey;



-- 1)
select
l_orderkey,
l_linenumber,
l_quantity
from
customer,
orders,
lineitem
where   c_custkey < 20000
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey 
	and l_partkey < 20
	and l_suppkey < 500;

-- 2)
select
l_orderkey,
l_linenumber,
l_quantity
from
customer,
orders,
lineitem
where   c_custkey < 9000
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
	and l_partkey < 20
	and l_suppkey < 50
	and l_linenumber < 20
	and l_quantity < 50
	and l_extendedprice between 20000 and 60000
	and l_tax < 5.08;

-- 3)
select
l_orderkey,
l_linenumber,
l_quantity
from
customer,
orders,
lineitem
where   c_custkey > 9000
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
	and l_partkey < 20
	and l_suppkey < 50
	and l_linenumber < 20
	and l_quantity < 50
	and l_extendedprice between 20000 and 160000
	and l_returnflag <> 'N'
	and l_linestatus = 'F';

-- 4)

select
l_orderkey,
l_linenumber,
l_quantity
from
customer,
orders,
lineitem
where   c_custkey > 9000
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
	and l_partkey < 20
	and l_suppkey < 50
	and l_linenumber < 20
	and l_quantity < 50
	and l_extendedprice between 2000 and 60000
	and l_returnflag <> 'N'
	and l_linestatus = 'F';

-- 5)

select
l_orderkey,
l_linenumber,
l_quantity
from
customer,
orders,
lineitem
where   c_custkey > 9000
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
 	and o_orderstatus = 'O'
	and o_totalprice between 900.00 and 21000.00
	and o_orderdate > date'1997-04-08'
	and o_orderpriority = '4-NOT SPECIFIED'
	and o_clerk > 'Clerk#000000152'
	and o_shippriority = '0';
