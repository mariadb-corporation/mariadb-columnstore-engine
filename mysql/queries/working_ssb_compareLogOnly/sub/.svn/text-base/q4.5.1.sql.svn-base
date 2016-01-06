-- 4.5.1 Complex Semi-Join Subtree query 1

select p_category, sum(lo_ordtotalprice), count(*)
from lineorder lo_outer, part
where p_partkey = lo_partkey
--  and lo_outer.lo_orderdate <= 19920211
  and lo_outer.lo_orderdate <= 19920101
  and exists (select *
       from customer, lineorder lo_inner
       where c_name < 'Customer#000000174'
       and c_custkey = lo_inner.lo_custkey
--       and lo_inner.lo_orderdate <= 19920131
       and lo_inner.lo_orderdate <= 19920101
       
and lo_outer.lo_orderkey = lo_inner.lo_orderkey
and lo_outer.lo_linenumber < lo_inner.lo_linenumber
)
group by 1 order by 1;

