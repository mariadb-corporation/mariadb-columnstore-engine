-- 3.8.1 Simple Semi-Join Nested query 1
select count(*)
from part
where p_partkey in (
       select lo_partkey
       from lineorder
       where lo_custkey in
       (select c_custkey
       from customer
where c_name = 'Customer#000000084'));

