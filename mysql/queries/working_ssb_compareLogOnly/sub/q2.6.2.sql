/*
2.6.2 Scalar Multiple column variation 2
Note that the subquery specifies 2 scalar values.
*/

select p_category, sum(lo_ordtotalprice), count(*)
from lineorder, part
where p_partkey = lo_partkey
  and (lo_custkey, p_partkey)
       = (select lo_custkey, max(lo_partkey)
       from customer, lineorder
       where c_name = 'Customer#000000074'
       and c_custkey = lo_custkey
group by 1)
group by 1 order by 1;

