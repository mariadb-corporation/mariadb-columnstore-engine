-- 3.3.2 Simple Semi-Join In clause query 2
select sum(lo_ordtotalprice), count(*)
       from lineorder
where lo_custkey not in (select c_custkey
       from customer
       where c_name = 'Customer#000000074');

