# Updates order rows.

update orders2 set o_orderpriority=concat('upd-', o_orderkey), o_custkey=-1 where o_orderdate <= '1992-01-31';
select count(distinct o_orderpriority) from orders2 where o_orderdate <= '1992-01-31';
select count(*) from orders2 where o_custkey = -1;
delete from orders2 where o_orderdate <= '1992-01-31';

select count(*) from orders2;
select count(o_orderkey) from orders2;
select count(o_custkey) from orders2;
select count(o_orderstatus) from orders2;
select count(o_totalprice) from orders2;
select count(o_orderdate) from orders2;
select count(o_orderpriority) from orders2;
select count(o_clerk) from orders2;
select count(o_shippriority) from orders2;
select count(o_comment) from orders2;

