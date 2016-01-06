-- 3.5.1 Simple Semi-Join Aggregation query 1
-- Negative test case as we do not currently allow aggregatoin in the sub query with an exists clause.
select sum(lo_ordtotalprice), count(*)
from lineorder
where exists (select max(c_custkey)
       from customer
     where c_name = 'Customer#000000074'
     and lo_custkey = c_custkey );

