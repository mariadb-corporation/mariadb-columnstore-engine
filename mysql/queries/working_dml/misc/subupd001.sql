/*
* Sets c_nationkey to the orders count for the customer, then selects the 100 customers with the most orders.
*/
set autocommit=0;

update customer,
(select o_custkey as custkey, count(*) as ordercount
from orders 
group by custkey) ords
set c_nationkey = ords.ordercount
where ords.custkey = c_custkey;

select c_custkey, c_nationkey
from customer
order by 2 desc, 1
limit 100;

rollback;


