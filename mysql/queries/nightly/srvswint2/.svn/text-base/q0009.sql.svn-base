-- From Jim's demo queries.

-- Call getstats() twice below before cache queries so that every third query is the "real"
-- one for parsing out the times.

select now();
select now();
select calflushcache();

-- q0009.1.d.
-- Join Compares: 1 Million x 800 Million (5.93s @ 4 PMs)
select p_mfgr, count(*), avg(ps_availqty), avg(p_retailprice),
avg(ps_supplycost) from part, partsupp where p_size = 50 and p_retailprice <
1250 and ps_partkey = p_partkey and ps_suppkey <= 1000000 and ps_partkey <= 4000000 group by p_mfgr
order by p_mfgr;

select calgetstats();
select now();

-- q0009.1.c.
select p_mfgr, count(*), avg(ps_availqty), avg(p_retailprice),
avg(ps_supplycost) from part, partsupp where p_size = 50 and p_retailprice <
1250 and ps_partkey = p_partkey and ps_suppkey <= 1000000 and ps_partkey <= 4000000 group by p_mfgr
order by p_mfgr;

select calgetstats();
select calflushcache();

-- q0009.2.d.
-- Join Compares: 1 Million x 600 Million (5.37s @ 4 PMs)
select p_mfgr, count(*), avg(ps_availqty), avg(p_retailprice),
avg(ps_supplycost) from part, partsupp where p_size = 50 and p_retailprice <
1250  and ps_partkey = p_partkey and ps_suppkey <= 7500000 and ps_partkey <= 4000000 group by p_mfgr
order by p_mfgr;

select calgetstats();
select now();

-- q0009.2.c.
select p_mfgr, count(*), avg(ps_availqty), avg(p_retailprice),
avg(ps_supplycost) from part, partsupp where p_size = 50 and p_retailprice <
1250  and ps_partkey = p_partkey and ps_suppkey <= 7500000 and ps_partkey <= 4000000 group by p_mfgr
order by p_mfgr;

select calgetstats();
select calflushcache();

-- q0009.3.d.
-- Join Compares: 100,000 x 500 Million (4.01s @ 4 PMs)
select c_nationkey, count(*), sum(o_totalprice) Revenue, avg(c_acctbal)
from customer, orders where c_acctbal > 9963 and c_nationkey < 5 and o_custkey
=  c_custkey and o_orderdate <= '1994-03-13' group by c_nationkey order by 1;

select calgetstats();
select now();

-- q0009.3.c.
select c_nationkey, count(*), sum(o_totalprice) Revenue, avg(c_acctbal)
from customer, orders where c_acctbal > 9963 and c_nationkey < 5 and o_custkey
=  c_custkey and o_orderdate <= '1994-03-13' group by c_nationkey order by 1;

select calgetstats();
select calflushcache(); 

-- q0009.4.d.
-- Join Compares: 100,000 x 400 Million (3.44s @ 4 PMs
select c_nationkey, count(*), sum(o_totalprice) Revenue, avg(c_acctbal)
from customer, orders where c_acctbal > 9963 and c_nationkey < 5 and o_custkey
= c_custkey and o_orderdate <= '1993-10-03' group by c_nationkey order by 1;

select calgetstats();
select now();

-- q0009.4.c.
select c_nationkey, count(*), sum(o_totalprice) Revenue, avg(c_acctbal)
from customer, orders where c_acctbal > 9963 and c_nationkey < 5 and o_custkey
= c_custkey and o_orderdate <= '1993-10-03' group by c_nationkey order by 1;

select calgetstats();
select calflushcache();

-- q0009.5.d.
-- Join Compares: 100,000 x 300 Million (2.76s @ 4 PMs)
select c_nationkey, count(*), sum(o_totalprice) Revenue, avg(c_acctbal)
from customer, orders where c_acctbal > 9963 and c_nationkey < 5 and o_custkey
= c_custkey and o_orderdate <= '1993-04-26'  group by c_nationkey order by 1;

select calgetstats();
select now();

-- q0009.5.c.
select c_nationkey, count(*), sum(o_totalprice) Revenue, avg(c_acctbal)
from customer, orders where c_acctbal > 9963 and c_nationkey < 5 and o_custkey
= c_custkey and o_orderdate <= '1993-04-26'  group by c_nationkey order by 1;

select calgetstats();
select calflushcache();

-- q0009.6.d.
-- Join Compares: 100,000 x 200 Million (2.19s @ 4 PMs) (Note-2 seconds approaches the minimal time to read tables)
select c_nationkey, count(*), sum(o_totalprice) Revenue, avg(c_acctbal)
from customer, orders where c_acctbal > 9963 and c_nationkey < 5 and o_custkey
= c_custkey and o_orderdate <=  '1992-02-16' group by c_nationkey order by 1;

select calgetstats();
select now();

-- q0009.6.c.
select c_nationkey, count(*), sum(o_totalprice) Revenue, avg(c_acctbal)
from customer, orders where c_acctbal > 9963 and c_nationkey < 5 and o_custkey
= c_custkey and o_orderdate <=  '1992-02-16' group by c_nationkey order by 1;

select calgetstats();
select calflushcache();

-- q0009.7.d.
-- Join Compares: 250,000 x 1 Billion (11.35s @ 4 PMs)
select o_orderpriority, max(l_shipdate), avg(o_totalprice), count(*)
from orders, lineitem where o_orderdate > '1998-08-01' and o_totalprice < 1365 
and o_orderkey = l_orderkey and l_shipdate >  '1998-08-01' and l_suppkey <
10000000 group by o_orderpriority order by o_orderpriority;

select calgetstats();
select now();

-- q0009.7.c.
select o_orderpriority, max(l_shipdate), avg(o_totalprice), count(*)
from orders, lineitem where o_orderdate > '1998-08-01' and o_totalprice < 1365 
and o_orderkey = l_orderkey and l_shipdate >  '1998-08-01' and l_suppkey <
10000000 group by o_orderpriority order by o_orderpriority;

select calgetstats();
select calflushcache();

-- q0009.8.d.
-- Join Compares: 250,000 x 800 Million (10.00s @ 4 PMs)
select o_orderpriority, max(l_shipdate), avg(o_totalprice), count(*)
from orders, lineitem where o_orderdate > '1998-08-01' and o_totalprice < 1365 
  and o_orderkey = l_orderkey and l_shipdate >  '1998-08-01' and l_suppkey <
8000000 group by o_orderpriority order by o_orderpriority;

select calgetstats();
select now();

-- q0009.8.c.
select o_orderpriority, max(l_shipdate), avg(o_totalprice), count(*)
from orders, lineitem where o_orderdate > '1998-08-01' and o_totalprice < 1365 
  and o_orderkey = l_orderkey and l_shipdate >  '1998-08-01' and l_suppkey <
8000000 group by o_orderpriority order by o_orderpriority;

select calgetstats();
select calflushcache();

-- q0009.9.d.
-- Join Compares: 250,000 x 600 Million (8.38s @ 4 PMs)
select o_orderpriority, max(l_shipdate), avg(o_totalprice), count(*)  from
orders, lineitem  where o_orderdate > '1998-08-01' and o_totalprice < 1365 and
o_orderkey = l_orderkey and l_shipdate >  '1998-08-01' and l_suppkey < 6000000
group by o_orderpriority order by o_orderpriority;

select calgetstats();
select now();

-- q0009.9.c.
select o_orderpriority, max(l_shipdate), avg(o_totalprice), count(*) 
from orders, lineitem  where o_orderdate > '1998-08-01' and o_totalprice < 1365
and o_orderkey = l_orderkey and l_shipdate >  '1998-08-01' and l_suppkey <
6000000 group by o_orderpriority order by o_orderpriority;

select calgetstats();
select calflushcache();
