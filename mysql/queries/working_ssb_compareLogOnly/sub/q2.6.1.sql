-- 2.6.1 Scalar Multiple column variation 1
select sum(lo_ordtotalprice), count(*)
       from lineorder
       where (lo_custkey,lo_custkey) = (select
        max(c_custkey), min(c_custkey)
       from customer
where c_name = 'Customer#000000074');

