-- 2.4.1 Scalar Aggregation query 1
select sum(lo_ordtotalprice), count(*)
       from lineorder
where lo_custkey <= (select max(c_custkey)
       from customer
where c_name = 'Customer#000000074');

