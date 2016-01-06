/*
2.10.2 Scalar optimizer query 2
Case 2 ( <> ) will process part first within the outer query step and then scan lineorder to apply the lo_custkey <> <derived attribute> filter and HJ in one streaming step.
*/

select p_category, sum(lo_ordtotalprice), count(*)
from lineorder, part
where p_partkey = lo_partkey
  and lo_custkey <> (select max(c_custkey)
       from customer, lineorder
       where c_name < 'Customer#000000074'
       and c_custkey = lo_custkey )
group by 1 order by 1;

