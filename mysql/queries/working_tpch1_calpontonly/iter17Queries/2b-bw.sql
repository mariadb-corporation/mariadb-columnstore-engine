-- 2) Filter
--   b) Add filters from orders (e.g. c_custkey < 100 and o_orderkey > 100) (5)

-- 1)
select
l_orderkey,
l_linenumber,
l_quantity
from
customer,
orders,
lineitem
where   c_custkey < 27
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey 
	and o_custkey < 20
	and o_orderstatus = 'O';

-- 2)
select
l_orderkey,
l_linenumber,
l_quantity
from
customer,
orders,
lineitem
where   c_custkey < 9000000
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
	and o_orderstatus = 'O'
	and o_totalprice between 900.00 and 8000.00
	and o_orderdate > date'1997-04-08';

-- 3)
select
l_orderkey,
l_linenumber,
l_quantity
from
customer,
orders,
lineitem
where   c_custkey < 9000000
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
 	and o_orderstatus = 'O'
	and o_totalprice between 900.00 and 2000.00
	and o_orderdate > date'1994-04-08'
	and o_orderpriority = '4-NOT SPECIFIED';

-- 4)

select
l_orderkey,
l_linenumber,
l_quantity
from
customer,
orders,
lineitem
where   c_custkey < 9000000
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
 	and o_orderstatus = 'O'
	and o_totalprice between 900.00 and 4000.00
	and date'1997-04-08' > o_orderdate
	and  '4-NOT SPECIFIED' = o_orderpriority
	and o_clerk > 'Clerk#000000152';

-- 5)

select
l_orderkey,
l_linenumber,
l_quantity,
l_comment
from
customer,
orders,
lineitem
where   c_custkey < 9000000
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
 	and o_orderstatus = 'O'
	and o_totalprice between 900.00 and 7000.00
	and o_orderdate > date'1997-04-08'
	and o_orderpriority = '4-NOT SPECIFIED'
	and 'Clerk#000000152' < o_clerk 
	and o_shippriority = '0'
	and l_comment > 'dog';
