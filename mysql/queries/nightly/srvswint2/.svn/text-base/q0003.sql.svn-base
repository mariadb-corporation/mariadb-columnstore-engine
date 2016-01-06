-- From Jim's Bench_HJ_Orders_Line.sql script.

/*
mysql> desc orders;
+-----------------+---------------+------+-----+---------+-------+
| Field           | Type          | Null | Key | Default | Extra |
+-----------------+---------------+------+-----+---------+-------+
| o_orderkey      | bigint(20)    | YES  |     | NULL    |       |
| o_custkey       | int(11)       | YES  |     | NULL    |       |
| o_orderstatus   | char(1)       | YES  |     | NULL    |       |
| o_totalprice    | decimal(12,2) | YES  |     | NULL    |       |
| o_orderdate     | date          | YES  |     | NULL    |       |
| o_orderpriority | char(15)      | YES  |     | NULL    |       |
| o_clerk         | char(15)      | YES  |     | NULL    |       |
| o_shippriority  | int(11)       | YES  |     | NULL    |       |
| o_comment       | varchar(79)   | YES  |     | NULL    |       |
+-----------------+---------------+------+-----+---------+-------+
9 rows in set (0.00 sec)

mysql> desc lineitem;
+-----------------+---------------+------+-----+---------+-------+
| Field           | Type          | Null | Key | Default | Extra |
+-----------------+---------------+------+-----+---------+-------+
| l_orderkey      | bigint(20)    | YES  |     | NULL    |       |
| l_partkey       | int(11)       | YES  |     | NULL    |       |
| l_suppkey       | int(11)       | YES  |     | NULL    |       |
| l_linenumber    | bigint(20)    | YES  |     | NULL    |       |
| l_quantity      | decimal(12,2) | YES  |     | NULL    |       |
| l_extendedprice | decimal(12,2) | YES  |     | NULL    |       |
| l_discount      | decimal(12,2) | YES  |     | NULL    |       |
| l_tax           | decimal(12,2) | YES  |     | NULL    |       |
| l_returnflag    | char(1)       | YES  |     | NULL    |       |
| l_linestatus    | char(1)       | YES  |     | NULL    |       |
| l_shipdate      | date          | YES  |     | NULL    |       |
| l_commitdate    | date          | YES  |     | NULL    |       |
| l_receiptdate   | date          | YES  |     | NULL    |       |
| l_shipinstruct  | char(25)      | YES  |     | NULL    |       |
| l_shipmode      | char(10)      | YES  |     | NULL    |       |
| l_comment       | varchar(44)   | YES  |     | NULL    |       |
+-----------------+---------------+------+-----+---------+-------+
16 rows in set (0.01 sec)
*/

-- -------------------------------------------------------------------------
-- Verify count at 250,000 for small side of join
-- -------------------------------------------------------------------------
-- select count(*) from orders where o_orderdate > '1998-08-01' and o_totalprice < 1365  ;
-- -------------------------------------------------------------------------

-- -------------------------------------------------------------------------

-- select count(*) '~200 Million Items' from lineitem where l_shipdate >  '1998-08-01' and l_suppkey <  2000000;

select now();
select now();
select calflushcache();

-- q0003.1.d
select o_orderpriority, max(l_shipdate), avg(o_totalprice), count(*) 
from orders, lineitem 
where o_orderdate > '1998-08-01' and o_totalprice < 1365  
	and o_orderkey = l_orderkey 
and l_shipdate >  '1998-08-01' and l_suppkey < 200000
group by o_orderpriority
order by o_orderpriority;

select calgetstats();
select now();

-- q0003.1.c
select o_orderpriority, max(l_shipdate), avg(o_totalprice), count(*) 
from orders, lineitem 
where o_orderdate > '1998-08-01' and o_totalprice < 1365  
	and o_orderkey = l_orderkey 
and l_shipdate >  '1998-08-01' and l_suppkey < 200000
group by o_orderpriority
order by o_orderpriority;

select calgetstats();
select calflushcache();

-- -------------------------------------------------------------------------

-- q0003.2.d
-- select count(*) '~400 Million Items' from lineitem where l_shipdate >  '1998-08-01' and l_suppkey <  4000000;
select o_orderpriority, max(l_shipdate), avg(o_totalprice), count(*) 
from orders, lineitem 
where o_orderdate > '1998-08-01' and o_totalprice < 1365  
	and o_orderkey = l_orderkey 
and l_shipdate >  '1998-08-01' and l_suppkey < 400000
group by o_orderpriority
order by o_orderpriority;

select calgetstats();
select now();

-- q0003.2.c
select o_orderpriority, max(l_shipdate), avg(o_totalprice), count(*) 
from orders, lineitem 
where o_orderdate > '1998-08-01' and o_totalprice < 1365  
	and o_orderkey = l_orderkey 
and l_shipdate >  '1998-08-01' and l_suppkey < 400000
group by o_orderpriority
order by o_orderpriority;

select calgetstats();
select calflushcache();

-- -------------------------------------------------------------------------
-- q0003.3.d
-- select count(*) '~600 Million Items' from lineitem where l_shipdate >  '1998-08-01' and l_suppkey <  6000000;

select o_orderpriority, max(l_shipdate), avg(o_totalprice), count(*) 
from orders, lineitem 
where o_orderdate > '1998-08-01' and o_totalprice < 1365  
	and o_orderkey = l_orderkey 
and l_shipdate >  '1998-08-01' and l_suppkey < 600000
group by o_orderpriority
order by o_orderpriority;

select calgetstats();
select now();

-- q0003.3.c
select o_orderpriority, max(l_shipdate), avg(o_totalprice), count(*) 
from orders, lineitem 
where o_orderdate > '1998-08-01' and o_totalprice < 1365  
	and o_orderkey = l_orderkey 
and l_shipdate >  '1998-08-01' and l_suppkey < 600000
group by o_orderpriority
order by o_orderpriority;

select calgetstats();
select calflushcache();

-- -------------------------------------------------------------------------
-- q0003.4.d
-- select count(*) '~800 Million Items' from lineitem where l_shipdate >  '1998-08-01' and l_suppkey <  8000000;

select o_orderpriority, max(l_shipdate), avg(o_totalprice), count(*) 
from orders, lineitem 
where o_orderdate > '1998-08-01' and o_totalprice < 1365  
	and o_orderkey = l_orderkey 
and l_shipdate >  '1998-08-01' and l_suppkey < 800000
group by o_orderpriority
order by o_orderpriority;

select calgetstats();
select now();

-- q0003.4.c
select o_orderpriority, max(l_shipdate), avg(o_totalprice), count(*) 
from orders, lineitem 
where o_orderdate > '1998-08-01' and o_totalprice < 1365  
	and o_orderkey = l_orderkey 
and l_shipdate >  '1998-08-01' and l_suppkey < 800000
group by o_orderpriority
order by o_orderpriority;

select calgetstats();
select now();
-- -------------------------------------------------------------------------

