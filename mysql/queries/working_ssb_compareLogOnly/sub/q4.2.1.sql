/*
Note that the subquery portion is:
1) Materialized before entry into the Semi-Join HJS
2) Includes a join to exactly one table in the main part of the query.
3) Includes one or more additional filters to the same table in the main part of the query.
4) The virtual columns within the materialized subquery can come from any table within that subtree without restriction.
a. These columns can be used for the join or any additional filters
b. There can be a compound (multi-column) join to the single table in the main part of the query.
c. The columns used for either joins or filters are virtual columns that can be derived from functions and expressions, from an aggregation operation, or as the result of a union operation.
4.2 Exists clause variations

4.2.1 Complex Semi-Join Exists clause query 1
*/

select * from lineorder l_outer where exists 
    ( select * from lineorder  l_subquery
	where l_subquery.lo_partkey = l_outer.lo_partkey
	  and l_subquery.lo_orderkey = l_outer.lo_orderkey
	  and l_subquery.lo_custkey <> l_outer.lo_custkey
        and l_subquery.lo_orderkey < 10);

-- Variation that doesn't return empty set.
select * from lineorder l_outer where l_outer.lo_orderkey <= 1000 and exists
    ( select * from lineorder  l_subquery
        where l_subquery.lo_shippriority = l_outer.lo_shippriority
          and l_subquery.lo_linenumber = l_outer.lo_linenumber
          and l_subquery.lo_custkey = l_outer.lo_custkey 
	  and l_subquery.lo_partkey <> l_outer.lo_partkey
        and l_subquery.lo_orderkey < 10) 
order by 1, 2;
