-- From Jim's Bench_Scan_Aggregate_7x.sql script.
/*
+---------------+---------------+------+-----+---------+-------+
| Field         | Type          | Null | Key | Default | Extra |
+---------------+---------------+------+-----+---------+-------+
| ps_partkey    | int(11)       | YES  |     | NULL    |       |
| ps_suppkey    | int(11)       | YES  |     | NULL    |       |
| ps_availqty   | int(11)       | YES  |     | NULL    |       |
| ps_supplycost | decimal(12,2) | YES  |     | NULL    |       |
| ps_comment    | varchar(199)  | YES  |     | NULL    |       |
+---------------+---------------+------+-----+---------+-------+
5 rows in set (0.00 sec)
*/

select now();
select now();
select calflushcache();

-- q0005.1.d
select count(ps_suppkey) 'Count 400 Million 4 byte Ints: From 800 Million Rows' from partsupp 	where ps_suppkey <= 500000;
select calgetstats();
select now();

-- q0005.1.c
select count(ps_suppkey) 'Count 400 Million 4 byte Ints: From 800 Million Rows' from partsupp 	where ps_suppkey <= 500000;
select calgetstats();
select calflushcache();

-- q0005.2.d
select count(ps_supplycost) 'Count 400 Million 8 byte Decimals: From 800 Million Rows'  from partsupp 	where ps_supplycost <= 501;
select calgetstats();
select now();

-- q0005.2.c
select count(ps_supplycost) 'Count 400 Million 8 byte Decimals: From 800 Million Rows'  from partsupp 	where ps_supplycost <= 501;
select calgetstats();
select calflushcache();

-- q0005.3.d
select count(*), avg(ps_availqty), sum(ps_availqty), avg(ps_supplycost), sum(ps_supplycost) from partsupp 	where ps_suppkey <= 5000000 and ps_partkey <= 10000000;
select calgetstats();
select now();

-- q0005..3.c
select count(*), avg(ps_availqty), sum(ps_availqty), avg(ps_supplycost), sum(ps_supplycost) from partsupp 	where ps_suppkey <= 5000000 and ps_partkey <= 10000000;
select calgetstats();
select calflushcache();

-- q0005.4.d
select count(*), avg(ps_availqty), sum(ps_availqty), avg(ps_supplycost), sum(ps_supplycost) from partsupp 	where ps_supplycost <= 501;
select calgetstats();
select now();

-- q0005.4.c
select count(*), avg(ps_availqty), sum(ps_availqty), avg(ps_supplycost), sum(ps_supplycost) from partsupp 	where ps_supplycost <= 501;
select calgetstats();
select calflushcache();

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
*/

-- q0005.5.d
select count(o_orderkey) 'Count 750M 8 byte BigInts: From 1.5 Billion Rows'   from orders 	where o_orderkey > 3000000000;
select calgetstats();
select now();

-- q0005.5.c
select count(o_orderkey) 'Count 750M 8 byte BigInts: From 1.5 Billion Rows'   from orders 	where o_orderkey > 3000000000;
select calgetstats();
select calflushcache();

-- q0005.6.d
select count(o_custkey) 'Count 750M 4 byte Ints: From 1.5 Billion Rows'  from orders 	where o_custkey <= 75000000;
select calgetstats();
select now();

-- q0005.6.c
select count(o_custkey) 'Count 750M 4 byte Ints: From 1.5 Billion Rows'  from orders 	where o_custkey <= 75000000;
select calgetstats();
select calflushcache();

-- q0005.7.d
select o_orderstatus, count(*), sum(o_totalprice), avg(o_totalprice) from orders where o_orderkey > 3000000000 group by o_orderstatus order by o_orderstatus;
select calgetstats();
select now();

-- q0005.7.c
select o_orderstatus, count(*), sum(o_totalprice), avg(o_totalprice) from orders where o_orderkey > 3000000000 group by o_orderstatus order by o_orderstatus;
select calgetstats();
select calflushcache();

-- q0005.8.d
select o_orderstatus, count(*), sum(o_totalprice), avg(o_totalprice) from orders where o_orderdate <= '1992-12-31' and o_custkey <= 7500000 group by o_orderstatus order by o_orderstatus;
select calgetstats();
select now();

-- q0005.8.c
select o_orderstatus, count(*), sum(o_totalprice), avg(o_totalprice) from orders where o_orderdate <= '1992-12-31' and o_custkey <= 7500000 group by o_orderstatus order by o_orderstatus;
select calgetstats();
select calflushcache();

/*
mysql> desc lineitem
    -> ;
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
16 rows in set (0.00 sec)
*/


-- q0005.9.d
select  count(l_linestatus) 'Count 21 Billion Char(1)s: From ~42 Billion Rows' from lineitem where l_shipdate <= '1992-12-31' and l_linestatus <> 'O';
select calgetstats();
select now();

-- q0005.9.c
select  count(l_linestatus) 'Count 21 Billion Char(1)s: From ~42 Billion Rows' from lineitem where l_shipdate <= '1992-12-31' and l_linestatus <> 'O';
select calgetstats();
select calflushcache();

-- q0005.10.d
select  count(l_suppkey) 'Count 21 Billion 4 byte Ints: From ~42 Billion Rows' from lineitem where l_shipdate <= '1992-12-31' and l_suppkey > 500000;
select calgetstats();
select now();

-- q0005.10.c
select  count(l_suppkey) 'Count 21 Billion 4 byte Ints: From ~42 Billion Rows' from lineitem where l_shipdate <= '1992-12-31' and l_suppkey > 500000;
select calgetstats();
select calflushcache();

-- q0005.11.d
select l_linestatus, l_returnflag, count(*) from lineitem where l_shipdate <= '1992-12-31' and l_linestatus <> 'O' group by l_linestatus, l_returnflag;
select calgetstats();
select now();

-- q0005.11.c
select l_linestatus, l_returnflag, count(*) from lineitem where l_shipdate <= '1992-12-31' and l_linestatus <> 'O' group by l_linestatus, l_returnflag;
select calgetstats();
select now();
