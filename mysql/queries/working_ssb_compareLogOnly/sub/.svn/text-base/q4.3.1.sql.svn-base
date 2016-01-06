-- 4.3.1 Complex Semi-Join In clause query 1
select * from lineorder l_outer
where lo_orderkey in
    ( select lo_orderkey from lineorder  l_subquery
        where l_subquery.lo_partkey = l_outer.lo_partkey
          and l_subquery.lo_custkey <> l_outer.lo_custkey
        and l_subquery.lo_orderkey < 10) order by 1, 2;

-- Variation that returns rows.
select * from lineorder l_outer
where lo_orderkey <= 10000 and lo_suppkey in
    ( select lo_suppkey from lineorder  l_subquery
        where l_subquery.lo_suppkey = l_outer.lo_suppkey
          and l_subquery.lo_linenumber = l_outer.lo_linenumber
	and l_subquery.lo_commitdate > l_outer.lo_commitdate
        and l_subquery.lo_orderkey < 10) order by 1, 2;

