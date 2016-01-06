-- 3.9.1 Simple Semi-Join compound query 1
select sum(lo_ordtotalprice), count(*) from lineorder where lo_custkey in (
       select c_custkey from customer
       where c_name < 'Customer#000000084' )
and lo_partkey in (
       select p_partkey from part
       where p_partkey between 1 and 100000 );

