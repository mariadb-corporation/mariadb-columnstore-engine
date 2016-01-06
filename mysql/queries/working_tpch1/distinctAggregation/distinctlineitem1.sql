select count(distinct l_linenumber) from lineitem;
select sum(distinct l_linenumber), avg(distinct l_linenumber), count(distinct l_linenumber) from lineitem;

select distinct l_partkey, l_suppkey, sum(distinct l_tax) from lineitem where l_orderkey < 10000 group by 1, 2 order by 1, 2;
select distinct l_linenumber, l_shipmode, sum(distinct l_tax), sum(distinct l_extendedprice), max(l_tax), max(l_extendedprice) from lineitem where l_orderkey < 10000 group by 1, 2 order by 1,2;

select sum(distinct l_partkey), avg(distinct l_partkey), count(distinct l_partkey) from lineitem;
select avg(distinct l_partkey), min(l_partkey), min(distinct l_partkey), max(l_partkey), max(distinct l_partkey) from lineitem;

select count(distinct l_suppkey) from lineitem;
select avg(distinct l_suppkey), count(distinct l_suppkey), sum(distinct l_suppkey) from lineitem;

select count(distinct l_quantity) from lineitem;
select avg(distinct l_quantity) from lineitem;

select count(distinct l_discount) from lineitem;
select count(distinct l_discount), avg(l_discount) from lineitem;

select sum(distinct l_tax), sum(l_tax) from lineitem;

select count(distinct l_returnflag) from lineitem;
select count(distinct l_returnflag), count(l_orderkey) from lineitem;

select count(distinct l_linestatus), count(l_comment) from lineitem;

select count(distinct l_shipinstruct) from lineitem;
select count(distinct l_shipmode) from lineitem;

select count(distinct l_partkey), count(distinct l_suppkey), avg(l_partkey), avg(l_suppkey) from lineitem;
select avg(distinct l_partkey), count( distinct l_partkey), sum(distinct l_partkey),
	avg(distinct l_suppkey), count(distinct l_partkey), sum(distinct l_suppkey) from lineitem;
select avg(distinct l_partkey), avg(l_suppkey) from lineitem;

select count(distinct l_returnflag), count(l_linestatus) from lineitem;
select count(distinct l_shipmode), count(distinct l_shipinstruct) from lineitem;
select avg(distinct l_discount), sum(distinct l_tax), avg(l_linenumber) from lineitem;
select sum(distinct l_discount), avg(distinct l_tax) from lineitem;

select count(distinct l_partkey), count(distinct l_suppkey), sum(distinct l_partkey), sum(distinct l_suppkey) from lineitem where l_orderkey < 10000;

select count(distinct l_shipmode), count(distinct l_shipinstruct), avg(l_linenumber) from lineitem where l_orderkey < 10000;
select sum(distinct l_discount), avg(distinct l_tax) from lineitem where l_orderkey < 10000;

