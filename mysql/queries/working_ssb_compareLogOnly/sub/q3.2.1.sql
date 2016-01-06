-- 3.2.1 Simple Semi-Join Example query 1
select sum(lo_ordtotalprice), count(*)
       from lineorder
       where lo_custkey in (select c_custkey
       from customer
       where c_name = 'Customer#000000074');

