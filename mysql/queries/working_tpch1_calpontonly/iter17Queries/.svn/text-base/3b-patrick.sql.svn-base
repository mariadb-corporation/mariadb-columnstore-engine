-- 1 
select
    p_name,
    l_partkey,
    c_name,
    o_orderkey,
    n_name
from
    customer,
    orders,
    lineitem,
    part,
    region,
    nation
where
    o_totalprice > 450000.0
    and o_orderkey = l_orderkey
    and p_partkey = l_partkey
    and n_nationkey = c_nationkey
    and n_regionkey = r_regionkey
    and c_custkey = o_custkey
    and r_regionkey = 2;

-- 2
select
    o_orderkey,
    p_name,
    l_partkey,
    c_name,
    s_name
from
    customer,
    orders,
    lineitem,
    supplier,
    part,
    nation
where
    o_totalprice > 450000.0
    and n_nationkey > 4
    and c_custkey = o_custkey
    and o_orderkey = l_orderkey
    and p_partkey = l_partkey
    and s_suppkey = l_suppkey
    and n_nationkey = c_nationkey;

-- 3
select
    o_orderkey,
    p_name,
    l_partkey,
    c_name,
    s_name
from
    customer,
    orders,
    lineitem,
    supplier,
    part,
    nation
where
    o_totalprice > 450000.0
    and n_nationkey > 4
    and c_custkey = o_custkey
    and o_orderkey = l_orderkey
    and p_partkey = l_partkey
    and s_suppkey = l_suppkey
    and n_nationkey = c_nationkey;

