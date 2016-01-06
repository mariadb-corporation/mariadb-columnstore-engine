-- 1.2.2
Select count(*) from orders where exists ( select * from lineitem where l_orderkey <> o_orderkey and l_shipdate > o_orderdate );
