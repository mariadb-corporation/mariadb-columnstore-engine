-- 4.4.1 Complex Semi-Join Aggregation query 1

select * from lineorder lo_outer
where lo_outer.lo_orderkey <= 200 and 
     (lo_orderkey, lo_linenumber) in
    ( select lo_orderkey, max(lo_linenumber)
       from lineorder lo_subquery
                where lo_orderkey < 10
and lo_outer.lo_discount =
        lo_subquery.lo_discount
                group by 1 ) order by 1, 2;

