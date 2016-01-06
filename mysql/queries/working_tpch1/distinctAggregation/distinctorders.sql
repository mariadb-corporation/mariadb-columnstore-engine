select count(distinct o_custkey), sum(distinct o_custkey) from orders;
select count(o_orderstatus), count(distinct o_orderstatus), sum(o_custkey), sum(distinct o_custkey) from orders;
select count(distinct o_orderpriority) from orders;
select count(distinct o_clerk) from orders;
select count(o_clerk), count(distinct o_shippriority) from orders;

select distinct o_orderstatus, count(*) from orders group by 1 order by 1;
select distinct o_orderstatus, count(distinct o_clerk), sum(o_custkey), sum(distinct o_custkey) from orders group by 1 order by 1;
select distinct count(distinct o_clerk), sum(o_custkey), avg(o_custkey), sum(distinct o_custkey), count(o_orderstatus) from orders;
select distinct o_orderstatus, o_orderpriority, count(distinct o_orderpriority) from orders group by 1, 2 order by 1, 2;
select distinct o_orderstatus, o_orderpriority, count(distinct o_orderpriority) from orders group by 1, 2 order by 1, 2;
select distinct o_orderstatus, o_clerk, o_orderpriority, count(distinct o_custkey) from orders group by 1, 2, 3 order by 1, 2, 3;
select min(o_orderpriority), max(o_orderpriority), count(distinct o_orderstatus), sum(distinct o_totalprice), avg(distinct o_totalprice) from orders;
select avg(distinct o_totalprice) from orders order by 1;
select count(distinct o_orderstatus), max(o_orderpriority) from orders where o_orderkey < 0 order by 1, 2;
select count(distinct o_orderstatus), count(o_clerk) from orders;
select count(o_clerk), count(o_orderstatus), count(o_orderkey), count(distinct o_orderstatus), count(o_clerk) as b from orders;
select count(distinct o_clerk), count(distinct o_shippriority) from orders;
select count(distinct o_clerk), count(distinct o_shippriority), count(o_clerk), count(o_shippriority), count(o_orderstatus) from orders;
select count(distinct o_clerk), count(o_orderstatus), count(distinct o_orderpriority), count(o_shippriority) from orders;
select count(distinct o_clerk), count(distinct o_orderstatus), count(distinct o_orderpriority), count(distinct o_shippriority) from orders;

select sum(distinct o_totalprice), count(o_totalprice), avg(distinct o_totalprice), avg(o_totalprice) from orders order by 1, 2, 3, 4;
