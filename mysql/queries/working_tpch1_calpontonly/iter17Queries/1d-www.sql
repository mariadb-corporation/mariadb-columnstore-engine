-- Project only columns not specified in the where clause (4).
select
c_name,
c_nationkey,
o_totalprice
from
customer,
orders,
lineitem
where   c_custkey < 6
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey;

select
o_totalprice,
c_acctbal,
o_orderstatus,
o_shippriority
from
customer,
orders,
lineitem
where   c_custkey < 6
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey;

select
o_orderdate,
o_orderpriority,
o_clerk,
o_shippriority,
o_comment,
l_receiptdate,
c_nationkey
from
customer,
orders,
lineitem
where   c_custkey < 6
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey;

select
c_comment,
l_shipdate
from
customer,
orders,
lineitem
where   c_custkey < 6
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey;

