-- 4.2.2 Complex Semi-Join Not Exists clause query 2
select * from lineorder l_outer 
where round(l_outer.lo_orderkey, -3) = l_outer.lo_orderkey and l_outer.lo_orderkey <= 100000 and 
not exists
    ( select * from lineorder  l_subquery
        where l_subquery.lo_partkey = l_outer.lo_partkey
          and l_subquery.lo_orderkey = l_outer.lo_orderkey
          and l_subquery.lo_custkey <> l_outer.lo_custkey
        and l_subquery.lo_orderkey < 10) order by 1, 2;

