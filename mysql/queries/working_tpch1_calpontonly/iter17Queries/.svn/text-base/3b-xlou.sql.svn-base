-- 1
select
        l_orderkey,
        o_orderkey,
        c_phone,
        n_name,
        r_name
from
        region,
        nation,
        customer,
        orders,
        lineitem
where   (c_name < 'Customer#000000003' or c_name > 'Customer#000139998')
        and c_custkey = o_custkey
        and (l_shipmode = 'air' and l_returnflag in ('A', 'R'))
        and r_regionkey = n_regionkey
        and n_nationkey > n_regionkey
        and o_orderkey = l_orderkey
        and c_nationkey = n_nationkey;


-- 2   
select
        l_orderkey,
        l_partkey,
        l_quantity,
        n_name,
        r_name
from
        region,
        nation,
        customer,
        orders,
        lineitem
where   n_nationkey > 10
        and c_custkey = o_custkey
        and r_regionkey > 1
        and o_orderkey = l_orderkey
        and not l_shipmode = 'air'
        and r_regionkey = n_regionkey
        and l_linestatus = 'O'
        and l_quantity > 1 and l_quantity < 5
        and c_nationkey = n_nationkey
        and (o_orderdate > '1998-07-31' or o_orderdate < '1993-01-10');


-- 3    
select
        o_orderkey,
        l_quantity,
        p_name
from
        nation,
        customer,
        part,
        orders,
        lineitem
where   c_custkey = o_custkey
        and p_size < 2 and p_container > 'MED'
        and o_orderkey = l_orderkey
        and o_orderkey > 5992800
        and p_partkey = l_partkey
        and c_acctbal < 0
        and c_nationkey = n_nationkey
        and (l_discount < 0.02 or l_discount > 0.38)
order by 1, 2;

