-- 3.4.1 Simple Semi-Join Exists query 1
-- Note that select * with exists does not require selecting all of the columns.

select sum(lo_ordtotalprice), count(*)
       from lineorder
       where exists (select *
       from customer
       where c_name = 'Customer#000000074'
       and lo_custkey = c_custkey );

