-- 2.9.1 Scalar compound query 1

select sum(lo_ordtotalprice), count(*) from lineorder where lo_custkey <= (
       select max(c_custkey) from customer
       where c_name < 'Customer#000000084' )
and lo_partkey <= (
       select avg(p_partkey) from part
       where p_partkey between 1 and 100000);

