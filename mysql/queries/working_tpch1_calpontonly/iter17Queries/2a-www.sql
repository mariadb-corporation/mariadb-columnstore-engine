-- 2) Filter
--   a) Add filters from customer (e.g. c_custkey < 100 and c_acctbal > 100) (4)

-- 1)
select
l_orderkey,
l_linenumber,
l_quantity
from
customer,
orders,
lineitem
where   c_custkey < 2000
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey 
	and c_nationkey < 20
	and c_acctbal > 25;

-- 2)
select
l_orderkey,
l_linenumber,
l_quantity
from
customer,
orders,
lineitem
where   c_custkey < 2456
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
        and c_nationkey < 20
	and c_name = 'Customer#000000001'
	and c_mktsegment > 'A';

-- 3)
select
l_orderkey,
l_linenumber,
l_quantity,
c_acctbal,
c_phone
from
customer,
orders,
lineitem
where   c_custkey < 5000
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
        and c_nationkey between 4 and 5 
	and c_acctbal >= 2866.83;

-- 4)

select
l_orderkey,
l_linenumber,
l_quantity,
c_phone
from
customer,
orders,
lineitem
where   c_custkey < 50000
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
        and c_acctbal >= 35.99 and
	c_phone = '25-989-741-2988';


