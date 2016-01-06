alter table lineitem add column l_orderkey2 bigint;

select 'q1', count(l_orderkey2) from lineitem;
select 'q2', count(*) from lineitem where l_orderkey2 is null;

update lineitem set l_orderkey2=l_orderkey;

select 'q3', count(l_orderkey2) from lineitem;
select 'q4', count(*) from lineitem where l_orderkey=l_orderkey2;

alter table lineitem drop column l_orderkey2;
select count(l_orderkey) from lineitem;
select count(l_partkey) from lineitem;
select count(l_suppkey) from lineitem;
select count(l_linenumber) from lineitem;
select count(l_quantity) from lineitem;
select count(l_extendedprice) from lineitem;
select count(l_discount) from lineitem;
select count(l_tax) from lineitem;
select count(l_returnflag) from lineitem;
select count(l_linestatus) from lineitem;
select count(l_shipdate) from lineitem;
select count(l_commitdate) from lineitem;
select count(l_receiptdate) from lineitem;
select count(l_shipinstruct) from lineitem;
select count(l_shipmode) from lineitem;
select count(l_comment) from lineitem;


