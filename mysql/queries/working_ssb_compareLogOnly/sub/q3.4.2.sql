/*
3.4.2 Simple Semi-Join Exists query 2
Note that select * with exists does not require selecting all of the columns.
*/

select sum(lo_ordtotalprice), count(*)
       from lineorder
       where not exists (select *
       from customer
       where c_name = 'Customer#000000074'
       and lo_custkey = c_custkey );


