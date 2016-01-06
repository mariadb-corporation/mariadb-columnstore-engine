select distinct o_custkey from orders order by 1;
select distinct o_orderstatus from orders order by 1;
select distinct o_orderpriority from orders order by 1;
select distinct o_clerk from orders order by 1;
select distinct o_shippriority from orders order by 1;

select distinct o_orderstatus, o_orderpriority from orders order by 1, 2;
select distinct o_orderstatus, o_shippriority from orders order by 1,2;
select distinct o_orderpriority, o_shippriority from orders order by 1,2;
select distinct o_orderstatus, o_clerk from orders order by 1,2;
select distinct o_clerk, o_shippriority from orders order by 1,2;
select distinct o_clerk, o_orderstatus, o_orderpriority, o_shippriority from orders order by 1,2,3,4;

select distinct o_custkey, o_orderstatus, o_orderpriority from orders where o_custkey < 1000 order by 1,2,3;
select distinct o_custkey, o_orderstatus, o_shippriority from orders where o_custkey < 1000 order by 1,2,3;
select distinct o_custkey, o_orderpriority, o_shippriority from orders where o_custkey < 1000 order by 1,2,3;
select distinct o_custkey, o_orderstatus, o_clerk from orders where o_custkey < 1000 order by 1,2,3;
select distinct o_custkey, o_clerk, o_shippriority from orders where o_custkey < 1000 order by 1,2,3;
select distinct o_custkey, o_clerk, o_orderstatus, o_orderpriority, o_shippriority from orders where o_custkey < 1000 order by 1,2,3,4,5;

