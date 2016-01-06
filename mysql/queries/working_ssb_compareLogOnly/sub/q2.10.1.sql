/*
2.10.1 Scalar optimizer query 1
Case 1 ( = ) will process lineorder first within the outer query step to apply the lo_custkey = <derived attribute> filter.
*/

select p_category, sum(lo_ordtotalprice), count(*)
from lineorder, part
where p_partkey = lo_partkey
  and lo_custkey = (select max(c_custkey)
       from customer, lineorder
       where c_name < 'Customer#000000074'
       and c_custkey = lo_custkey )
group by 1 order by 1;

