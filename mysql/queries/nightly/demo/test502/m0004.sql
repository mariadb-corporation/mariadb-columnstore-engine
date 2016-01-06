set autocommit=0;
delete from orders2 where o_orderdate <= '1992-04-30';
rollback;

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

delete from orders2 where o_orderdate <= '1992-05-31';
commit;

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
