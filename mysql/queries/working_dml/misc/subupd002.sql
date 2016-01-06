set autocommit=0;

update nation 
set n_regionkey = (select o_custkey from orders where o_orderkey = n_nationkey and o_orderkey <= 40);

select n_nationkey, n_regionkey from nation order by 1;

rollback;
