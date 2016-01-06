-- 3.6.1 Simple Semi-Join Subtree query 1
select p_category, sum(lo_ordtotalprice), count(*)
from lineorder l_outer, part
where p_partkey = lo_partkey
  and exists (select *
       from customer, lineorder l_inner
       where c_name < 'Customer#000000074'
       and c_custkey = l_inner.lo_custkey
and l_outer.lo_orderkey = l_inner.lo_orderkey)
group by 1 order by 1;

