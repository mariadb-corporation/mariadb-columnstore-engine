-- 4.4.2 Complex Semi-Join Aggregation query 2
select lo_orderkey, lo_linenumber, lo_discount
from lineorder lo_outer
where lo_outer.lo_orderkey <= 500 and (lo_orderkey, lo_linenumber) in
    ( select lo_orderkey, max(lo_linenumber)
                from lineorder lo_subquery
		where lo_subquery.lo_orderkey <= 1000
                group by 1
                having max(lo_discount)
       = lo_outer.lo_discount ) order by 1, 2;

