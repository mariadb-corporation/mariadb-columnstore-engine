-- 2.8.1 Scalar Nested query 1

select sum(lo_ordtotalprice), count(*) from lineorder where lo_custkey <= (
       select max(c_custkey) from customer
       where c_name < 'Customer#000000084'
       and c_custkey not in (
                select c_custkey from customer
       where c_custkey between 74 and 100)
       );

