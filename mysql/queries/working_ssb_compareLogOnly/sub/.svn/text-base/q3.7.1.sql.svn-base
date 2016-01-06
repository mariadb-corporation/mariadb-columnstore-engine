-- 3.7.1 Simple Semi-Join Multiple column query 1
select * from lineorder l_outer where exists
    ( select * from lineorder  l_subquery
        where l_subquery.lo_partkey = l_outer.lo_partkey
          and l_subquery.lo_custkey = l_outer.lo_custkey
        and l_subquery.lo_orderkey < 10);

-- Processed with a compound join.

