-- Scalar Example query 1
select sum(lo_ordtotalprice), count(*)
from lineorder where lo_custkey =
        (select c_custkey from customer
where c_name = 'Customer#000000074');

