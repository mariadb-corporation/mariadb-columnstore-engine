set autocommit=0;

# Set c_nationkey
select count(*) from customer where c_custkey in (select o_custkey from orders);
update customer set c_nationkey=-1 where c_custkey in (select o_custkey from orders);
select count(*) from customer where c_nationkey=-1;
rollback;
select count(*) from customer where c_nationkey=-1;

# Set c_nationkey to the order count for each customer.
update customer set c_nationkey = null;
update customer set c_nationkey = (select count(*) from orders where o_custkey=c_custkey);
select c_nationkey, count(*) from customer group by 1 order by 1;
rollback;
select c_nationkey, count(*) from customer group by 1 order by 1;

