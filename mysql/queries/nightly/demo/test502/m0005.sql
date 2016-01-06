set autocommit=0;

delete from orders2 where o_orderdate <= '1992-08-31';
rollback;

update orders2 set o_orderdate='2015-01-01' where o_orderdate <= '1992-08-31';
commit;
delete from orders2 where o_orderdate = '2015-01-01';
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

delete from orders2;
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
