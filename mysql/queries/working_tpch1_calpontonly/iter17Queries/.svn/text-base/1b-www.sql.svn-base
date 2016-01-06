-- Remove columns from lineitem (1).
select
l_linenumber,
l_quantity
from
customer,
orders,
lineitem
where   c_custkey < 2
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey;

