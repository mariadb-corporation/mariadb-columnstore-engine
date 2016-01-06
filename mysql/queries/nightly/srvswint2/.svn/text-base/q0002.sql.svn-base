-- From Jim's Bench_HJ_Cust_Orders.sql script.
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

mysql> desc customer;
+--------------+---------------+------+-----+---------+-------+
| Field        | Type          | Null | Key | Default | Extra |
+--------------+---------------+------+-----+---------+-------+
| c_custkey    | int(11)       | YES  |     | NULL    |       |
| c_name       | varchar(25)   | YES  |     | NULL    |       |
| c_address    | varchar(40)   | YES  |     | NULL    |       |
| c_nationkey  | int(11)       | YES  |     | NULL    |       |
| c_phone      | char(15)      | YES  |     | NULL    |       |
| c_acctbal    | decimal(12,2) | YES  |     | NULL    |       |
| c_mktsegment | char(10)      | YES  |     | NULL    |       |
| c_comment    | varchar(117)  | YES  |     | NULL    |       |
+--------------+---------------+------+-----+---------+-------+
8 rows in set (0.01 sec)
*/

-- -------------------------------------------------------------------------
-- Verify count at 100,000 for small side of join
-- -------------------------------------------------------------------------
-- select count(*) 'Small Side Join Count' from customer where c_acctbal > 9963 and c_nationkey < 5;
-- -------------------------------------------------------------------------
select now();
select now();
select calflushcache();
-- -------------------------------------------------------------------------

-- q0002.1.d
-- select count(*) '100 Million Orders' from orders where o_orderdate <= '1992-06-09';

select c_nationkey, count(*), sum(o_totalprice) Revenue, avg(c_acctbal)
from customer, orders 
where c_acctbal > 9963 and c_nationkey < 5
and o_custkey = c_custkey
and o_orderdate <= '1992-03-09'
group by c_nationkey
order by 1;

-- Call getstats() twice so that every third query is the one to get the time.  
select calgetstats();
select now();

-- q0002.1.c
select c_nationkey, count(*), sum(o_totalprice) Revenue, avg(c_acctbal)
from customer, orders 
where c_acctbal > 9963 and c_nationkey < 5
and o_custkey = c_custkey
and o_orderdate <= '1992-03-09'
group by c_nationkey
order by 1;
select calgetstats();
select calflushcache();

-- -------------------------------------------------------------------------

-- q0002.2.d
-- select count(*) '200 Million Orders' from orders where o_orderdate <= '1992-08-16';

select c_nationkey, count(*), sum(o_totalprice) Revenue, avg(c_acctbal)
from customer, orders 
where c_acctbal > 9963 and c_nationkey < 5
and o_custkey = c_custkey
and o_orderdate <=  '1992-11-16'
group by c_nationkey
order by 1;

select calgetstats();
select now();

-- q0002.2.c
select c_nationkey, count(*), sum(o_totalprice) Revenue, avg(c_acctbal)
from customer, orders 
where c_acctbal > 9963 and c_nationkey < 5
and o_custkey = c_custkey
and o_orderdate <=  '1992-08-16'
group by c_nationkey
order by 1;

select calgetstats();
select calflushcache();

-- -------------------------------------------------------------------------

-- select count(*) '300 Million Orders' from orders where o_orderdate <= '1992-11-26';
-- q0002.3.d
select c_nationkey, count(*), sum(o_totalprice) Revenue, avg(c_acctbal)
from customer, orders 
where c_acctbal > 9963 and c_nationkey < 5
and o_custkey = c_custkey
and o_orderdate <= '1993-04-26' 
group by c_nationkey
order by 1;

select calgetstats();
select now();

-- q0002.3.c
select c_nationkey, count(*), sum(o_totalprice) Revenue, avg(c_acctbal)
from customer, orders 
where c_acctbal > 9963 and c_nationkey < 5
and o_custkey = c_custkey
and o_orderdate <= '1992-11-26' 
group by c_nationkey
order by 1;

select calgetstats();
select calflushcache();

-- -------------------------------------------------------------------------

-- select count(*) '400 Million Orders' from orders where o_orderdate <= '1993-01-03';
-- q0002.4.d
select c_nationkey, count(*), sum(o_totalprice) Revenue, avg(c_acctbal)
from customer, orders 
where c_acctbal > 9963 and c_nationkey < 5
and o_custkey = c_custkey
and o_orderdate <= '1993-10-03' 
group by c_nationkey
order by 1;

select calgetstats();
select now();

-- q0002.4.c
select c_nationkey, count(*), sum(o_totalprice) Revenue, avg(c_acctbal)
from customer, orders 
where c_acctbal > 9963 and c_nationkey < 5
and o_custkey = c_custkey
and o_orderdate <= '1993-01-03' 
group by c_nationkey
order by 1;

select calgetstats();
select calflushcache();

-- -------------------------------------------------------------------------

-- select count(*) '500 Million Orders' from orders where o_orderdate <= '1994-03-13';

-- q0002.5.d
select c_nationkey, count(*), sum(o_totalprice) Revenue, avg(c_acctbal)
from customer, orders 
where c_acctbal > 9963 and c_nationkey < 5
and o_custkey = c_custkey
and o_orderdate <= '1993-08-13'
group by c_nationkey
order by 1;

select calgetstats();
select now();

-- q0002.5.c
select c_nationkey, count(*), sum(o_totalprice) Revenue, avg(c_acctbal)
from customer, orders 
where c_acctbal > 9963 and c_nationkey < 5
and o_custkey = c_custkey
and o_orderdate <= '1993-08-13'
group by c_nationkey
order by 1;

select calgetstats();
select now();

-- -------------------------------------------------------------------------
-- -------------------------------------------------------------------------
