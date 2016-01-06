-- 1    3 rows in set (1.70 sec)
select
        l_orderkey,
        l_partkey,
        l_quantity,
        c_name
from
        customer,
        part,
        orders,
        lineitem
where   (c_custkey < 6 or c_custkey > 149998)
        and l_quantity = 10
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
        and p_partkey = l_partkey
order by 1, 2;


-- 2    33 rows in set (0.00 sec)
select
        l_orderkey,
        l_partkey,
        l_quantity,
        c_name
from
        customer,
        part,
        orders,
        lineitem
where   (c_custkey > 0 and c_custkey < 3)
        and (l_shipmode = 'air' or l_returnflag > 'A')
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
        and p_partkey = l_partkey
order by 1, 2;


-- 3    109 rows in set (20.89 sec)
select
        c_name,
        n_name
from
        nation,
        customer,
        orders,
        lineitem
where   o_orderdate < '1996-12-05' and o_orderdate > '1996-12-01'
        and l_shipdate < '1996-12-06'
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
        and n_nationkey = c_nationkey
order by 1, 2;


-- 4    82 rows in set (0.14 sec)
select
        l_orderkey,
        l_quantity,
        c_name
from
        customer,
        part,
        orders,
        lineitem
where   (c_custkey < 2 or c_custkey > 149998)
        and c_custkey = o_custkey
        and p_partkey = l_partkey
        and (l_shipmode = 'air' or l_returnflag > 'A')
        and o_orderkey = l_orderkey
order by 1, 2;


-- 5    147 rows in set (3 min 10.83 sec)
select
        c_name,
        o_custkey,
        p_brand
from
        customer,
        part,
        orders,
        lineitem
where   c_custkey = o_custkey
        and (p_size < 2 or p_size > 49)
        and o_orderkey = l_orderkey
        and (l_receiptdate > '1996-12-01' and l_receiptdate < '1996-12-05')
        and (l_shipdate > l_commitdate)
        and p_partkey = l_partkey
order by 1, 2;


-- 6    2 rows in set (2.41 sec)
select
        l_orderkey,
        o_totalprice,
        p_mfgr
from
        customer,
        part,
        orders,
        lineitem
where   p_partkey = l_partkey
        and (l_shipmode = 'air' or l_returnflag in ('A', 'R'))
        and c_custkey = o_custkey
		and o_totalprice < 5000.00
        and o_orderkey = l_orderkey
        and o_orderdate between '1996-12-01' and '1996-12-02'
order by 1, 2;


-- 7    1 row in set (19.50 sec)
select
        count(c_custkey)
from
        nation,
        customer,
        orders,
        lineitem
where   o_orderdate < '1996-12-12' and o_orderdate > '1996-12-01'
        and c_custkey = o_custkey
        and l_shipdate > '1996-12-10' and l_shipdate < '1996-12-12'
        and o_orderkey = l_orderkey
        and (n_nationkey > 22 or n_nationkey < 3)
        and n_nationkey = c_nationkey;


-- 8    26 rows in set (1 min 3.34 sec)
select
        l_orderkey,
        o_totalprice,
        c_mktsegment
from
        customer,
        part,
        orders,
        lineitem
where   (c_name > 'Customer#000149988' and c_name < 'Customer#000149998')
        and c_custkey = o_custkey
        and p_size < 10
        and p_partkey = l_partkey
        and l_returnflag not in ('A', 'R')
        and o_orderkey = l_orderkey
order by 1, 2;


-- 9    1 row in set (1 min 0.54 sec)
select
        count(*)
from
        nation,
        customer,
        orders,
        lineitem
where   (c_name < 'Customer#000000003' or c_name > 'Customer#000149998')
        and c_custkey = o_custkey
        and (l_shipmode = 'air' and l_returnflag in ('A', 'R'))
        and n_nationkey > n_regionkey
        and o_orderkey = l_orderkey
--        conflicts with c_name OR
--        and c_mktsegment = 'AUTOMOBILE'
        and o_orderpriority like '%HIGH'
-- Commented out below line until like implemented as function
--        and (o_orderpriority like '%HIGH' or o_orderpriority like '%MEDIU')
        and c_nationkey = n_nationkey;


-- 10   43 rows in set (0.37 sec)
select
        l_orderkey,
        l_partkey,
        l_quantity,
        n_name
from
        nation,
        customer,
        orders,
        lineitem
where   n_nationkey > 20 and n_nationkey < 24
        and c_custkey = o_custkey
		and not (c_acctbal > 10.00 and c_acctbal < 9990.00)
        and o_orderkey = l_orderkey
        and not l_shipmode = 'air'
        and l_linestatus = 'O'
        and c_nationkey = n_nationkey
        and (o_orderdate > '1998-07-31' or o_orderdate < '1992-01-10')
order by 1, 2;


