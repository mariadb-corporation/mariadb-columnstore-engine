-- 1

select
	l_partkey,
	o_orderkey,
	p_name,
	c_name,
	n_name
from
	customer,
	orders,
	lineitem,
	nation,
	part
where
	o_totalprice > 450000.0
	-- work around for MySQL contant propagation optimization.
	-- and n_nationkey = 4
	and n_nationkey in (4, 4)
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey
	and n_nationkey = c_nationkey
	and l_partkey = p_partkey
order by 1, 2;

-- 2
select
    o_orderkey,
    p_name,
    l_partkey,
    c_name
from
    customer,
    orders,
    lineitem,
    nation,
    part
where
    o_totalprice > 450000.0
    -- work around for MySQL contant propagation optimization.
    -- and n_nationkey = 4
    and n_nationkey in (4, 4)
    and c_custkey = o_custkey
    and o_orderkey = l_orderkey
    and n_nationkey = c_nationkey
    and p_partkey = l_partkey
order by 1, 2;

-- 3
select
    o_orderkey,
    c_name,
	ps_partkey
from
    customer,
    orders,
	supplier,
    nation,
    partsupp
where
    o_totalprice > 450000.0
    -- work around for MySQL contant propagation optimization.
    -- and n_nationkey = 4
    and n_nationkey in (4, 4)
    and ps_availqty < 10
    and c_custkey = o_custkey
    and n_nationkey = c_nationkey
    and n_nationkey = s_nationkey
    and ps_suppkey = s_suppkey;

-- 4
select
    o_orderkey,
    c_name
from
    customer,
    orders,
	lineitem,
    supplier,
    nation
where
    o_totalprice > 450000.0
    -- work around for MySQL contant propagation optimization.
    -- and n_nationkey = 4
    and n_nationkey in (4, 4)
    and c_custkey = o_custkey
    and n_nationkey = c_nationkey
    and n_nationkey = s_nationkey
	and s_suppkey = l_suppkey
	and o_orderdate between '1993-01-01' and '1994-01-01'
order by 1;

-- 5
select
    p_name,
    l_partkey,
    c_name,
    o_orderkey,
    s_name
from
    customer,
    orders,
    lineitem,
    supplier,
    part
where
    o_totalprice > 450000.0
    and c_custkey = o_custkey
    and o_orderkey = l_orderkey
    and p_partkey = l_partkey
    and s_suppkey = l_suppkey
order by 1, 2;

-- 6
select
    p_name,
    l_partkey,
    c_name,
    o_orderkey,
    s_name
from
    customer,
    orders,
    lineitem,
    supplier,
    part
where
    o_totalprice > 450000.0
    and o_orderkey = l_orderkey
    and p_partkey = l_partkey
	and s_nationkey = 4
    and s_suppkey = l_suppkey
    and c_custkey = o_custkey
order by 1, 2;

-- 7
select
	n_nationkey,
	c_custkey,
	p_retailprice
from
    customer,
    orders,
    lineitem,
	part,
    nation
where
    o_totalprice > 450000.0
    and c_custkey = o_custkey
    and n_nationkey = c_nationkey
	and o_orderkey = l_orderkey
	and p_partkey = l_partkey
    and l_shipdate between '1993-01-01' and '1994-01-01'
order by 1;

-- 8
select
	p_size
from
	region,
	nation,
	supplier,
	partsupp,
	part
where
	r_regionkey = n_regionkey
	and n_nationkey = s_nationkey
	and s_suppkey = ps_suppkey
	and p_partkey = ps_partkey
	-- work around for MySQL contant propagation optimization.
	-- and s_nationkey = 10
	and s_nationkey in (10, 10)
	and p_retailprice < 1000
;

-- 9
select
	l_linenumber,
    p_size
from
    nation,
    supplier,
    partsupp,
    part,
	lineitem
where
    n_nationkey = s_nationkey
    and s_suppkey = ps_suppkey
    and p_partkey = ps_partkey
	and p_partkey = l_partkey
    and p_retailprice < 200
order by 1;

-- 10
select
	o_orderkey,
    p_partkey
from
    nation,
    supplier,
    partsupp,
    part,
	orders,
    lineitem
where
    n_nationkey = s_nationkey
    and s_suppkey = ps_suppkey
    -- work around for MySQL contant propagation optimization.
    -- and s_nationkey = 10
    and s_nationkey in (10, 10)
    and l_partkey = p_partkey
    and o_orderkey = l_orderkey
    and p_partkey = ps_partkey
order by 1;


