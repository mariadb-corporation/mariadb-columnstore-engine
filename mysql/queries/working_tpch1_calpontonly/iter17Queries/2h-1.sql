select count(*) from part where p_type < 100 or p_retailprice < 44706 or p_size <= 1;
SELECT count(*) from lineitem WHERE l_partkey < 100 OR l_linestatus = 'F';
select
        l_orderkey,
        l_quantity,
        l_extendedprice
from
        customer,
        orders,
        lineitem
where
        c_custkey < 100
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey;
select count(*) from supplier where s_comment != 'test' and  s_comment != 'dave';
select count(*) from orders where o_orderkey > 100 or o_shippriority != 1;
select count(*) from orders where o_orderkey > 100 or o_clerk != 'test';
select count(*) from orders where o_orderkey > 100 and o_orderkey < 100000;
select count(*) from region where r_regionkey = 100 or r_name != 'test';
select * from region where r_name = 'AFRICA' or r_name = 'ASIA' or r_regionkey in (1, 2, 3);
select * from region where r_regionkey  not in (2, 3) or r_name = 'ASIA';
select
        count(*)
from
        lineitem
where
        l_orderkey != 4
        or l_partkey != 3
        or l_suppkey != 987654
        or l_linenumber != 2
        or l_quantity != 6754
        or l_extendedprice != 12
--      or l_discountnumber != 321
--      or l_taxnumber != 1
        or l_returnFlag != 1
        or l_linestatus != 2
        or l_shipdate != '1993-01-01'
        or l_commitdate != '1993-01-01'
        or l_receiptdate !=  '1993-01-01'
        or l_shipinstruct != 'go'
        or l_shipmode != 'sent'
        or l_comment != 'test'
;
