-- 4.3.2 Complex Semi-Join Not In clause query 2
select * from lineorder l_outer
where round(lo_orderkey, -3) = lo_orderkey and lo_orderkey <= 40000 and lo_orderkey not in
    ( select lo_orderkey from lineorder  l_subquery
        where l_subquery.lo_partkey = l_outer.lo_partkey
          and l_subquery.lo_custkey <> l_outer.lo_custkey
        and l_subquery.lo_orderkey < 10) order by 1,2,4;

