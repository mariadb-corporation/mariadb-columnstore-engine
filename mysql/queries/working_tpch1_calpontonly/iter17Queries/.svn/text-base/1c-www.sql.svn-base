-- Project only the columns specified in the where clause (1).
select
l_orderkey,
o_orderkey,
c_custkey,
o_custkey
from
customer,
orders,
lineitem
where   c_custkey < 2
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey;

