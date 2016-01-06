-- Add columns from customers and/or orders to projection list (4).
select
l_orderkey,
l_linenumber,
l_quantity,
l_extendedprice,
c_custkey,
o_orderstatus
from
customer,
orders,
lineitem
where   c_custkey < 2
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
order by 1, 2;

select
l_extendedprice,
l_linenumber,
l_orderkey,
c_name,
c_address,
c_custkey,
o_orderstatus,
c_comment
from
customer,
orders,
lineitem
where   c_custkey > 149500
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey;

select
c_phone,
l_linenumber,
l_orderkey,
o_custkey,
o_orderstatus
o_orderstatus,
c_comment
from
customer,
orders,
lineitem
where   c_custkey <= 200 
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey;

select
l_orderkey,
l_linenumber,
c_address,
o_totalprice,
o_custkey,
c_custkey
from
customer,
orders,
lineitem
where   c_custkey < 2
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey;

