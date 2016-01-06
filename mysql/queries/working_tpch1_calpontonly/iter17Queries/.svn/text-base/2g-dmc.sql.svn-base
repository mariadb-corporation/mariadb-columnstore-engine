-- Use only and's on one table and only or's on another table
-- Example (c_custkey<100 and c_acctbal>100) and (o_orderstatus='F' or o_orderdate,'1995-12-31')

-- TEST01:
select
    l_orderkey,
    l_quantity,
    l_extendedprice,
    c_custkey,
    c_acctbal,
    c_nationkey
from
    customer,
    orders,
    lineitem
where
    ((c_custkey < 1000) and (c_acctbal < 500)) and
    (l_partkey <= 100 or l_linenumber = 1) and
    c_custkey  = o_custkey and
    o_orderkey = l_orderkey; 

-- TEST02:
select
    l_orderkey,
    l_quantity,
    l_extendedprice,
    c_custkey,
    c_acctbal
from
    customer,
    orders,
    lineitem
where
    ((c_custkey = 20) or  (c_custkey = 22) or (c_custkey = 23) or (c_custkey = 24)) and
    ((l_partkey > 100) and (l_partkey < 40000)) and
    c_custkey  = o_custkey and
    o_orderkey = l_orderkey; 

-- TEST03:
select
    l_orderkey,
    l_quantity,
    l_extendedprice,
    c_custkey,
    c_acctbal,
    o_orderstatus,
    o_orderpriority
from
    customer,
    orders,
    lineitem
where
    ((o_orderstatus = 'F') and (o_orderpriority = '1-URGENT')) and o_orderkey >= 20000 and o_orderkey < 24200 and
    ((c_custkey = 1) or (c_acctbal > 0)) and
    c_custkey  = o_custkey and
    o_orderkey = l_orderkey; 

-- TEST04:
select
    l_orderkey,
    l_quantity,
    l_extendedprice,
    c_custkey
    c_acctbal,
    o_orderstatus,
    o_clerk
from
    customer,
    orders,
    lineitem
where
    ((c_custkey = 2) or (c_acctbal < -500)) and
    ((o_orderstatus = 'F') and (o_clerk = 'Clerk#000000243')) and
    c_custkey  = o_custkey and
    o_orderkey = l_orderkey; 

-- TEST05: 2 Number ORs, and 1 Date/string OR
select
    l_orderkey,
    l_quantity,
    l_extendedprice,
    c_custkey,
    c_acctbal,
    o_orderdate,
    o_clerk
from
    customer,
    orders,
    lineitem
where
    ((c_phone > '33') and (c_acctbal < 1000)) and
    ((o_orderdate = '1992-01-29') or (o_clerk = 'Clerk#000000001')) and
    c_custkey  = o_custkey and
    o_orderkey = l_orderkey; 

-- TEST06:
select
    l_orderkey,
    l_quantity,
    l_extendedprice,
    c_custkey,
    c_acctbal,
    c_nationkey,
    o_orderkey,
    o_totalprice
from
    customer,
    orders,
    lineitem
where
    ((c_custkey < 100) and (c_nationkey = 23) and (c_acctbal > 59000)) and
    ((o_orderkey = 1526534) or (o_totalprice > 200000.00)) and
    c_custkey  = o_custkey and
    o_orderkey = l_orderkey; 

-- TEST07:
select
    l_orderkey,
    l_quantity,
    l_extendedprice,
    o_orderdate,
    o_shippriority,
    c_mktsegment,
    c_nationkey
from
    customer,
    orders,
    lineitem
where
    ((o_orderkey < 20000) and (o_orderdate = '1996-03-14')) and
    ((c_mktsegment = 'MACHINERY') or (c_nationkey = 18)) and
    c_custkey  = o_custkey and
    o_orderkey = l_orderkey;

-- TEST08:
select
    l_orderkey,
    l_quantity,
    l_extendedprice,
    o_orderdate,
    o_shippriority,
    c_mktsegment,
    c_nationkey,
    c_phone,
    o_orderstatus,
    c_name
from
    customer,
    orders,
    lineitem
where
    ((c_nationkey > 9) or (c_name = 'Customer#000013741') or (c_name = 'Customer#000013742')) and
    ((o_orderkey < 5200) and (o_orderstatus = 'F')) and
    c_custkey  = o_custkey and
    o_orderkey = l_orderkey;
