-- Reworked to use lineitem instead of demographics200.
select count(*),max(l_quantity),max(l_extendedprice),max(l_discount),
max(l_tax),max(l_shipinstruct),max(l_commitdate),max(l_receiptdate)
from lineitem 
where 		l_orderkey < 7009
		and l_suppkey < 70000
		and l_linenumber < 5
		and l_partkey < 7000
		and l_tax < 70500
		and l_extendedprice < 70500
		and l_discount < 28 ;
