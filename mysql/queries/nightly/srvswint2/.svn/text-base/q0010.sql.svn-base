-- From Jim's demo queries.  Modified to perform UM joins.

-- Call getstats() twice below before cache queries so that every third query is the "real"
-- one for parsing out the times.

select now();
select now();
select calflushcache();

-- q0010.1.d.
-- Join Compares: 3 Million x 800 Million (5.93s @ 4 PMs)
select p_mfgr, count(*), avg(ps_availqty), avg(p_retailprice),
avg(ps_supplycost) from part, partsupp where p_size in (49, 50) and p_retailprice <
1250 and ps_partkey = p_partkey and ps_suppkey <= 1000000 and ps_partkey <= 4000000 and p_partkey <= 4000000 group by p_mfgr
order by p_mfgr;

select calgetstats();
select now();

-- q0010.1.c.
select p_mfgr, count(*), avg(ps_availqty), avg(p_retailprice),
avg(ps_supplycost) from part, partsupp where p_size in (49, 50) and p_retailprice <
1250 and ps_partkey = p_partkey and ps_suppkey <= 1000000 and ps_partkey <= 4000000 and p_partkey <= 4000000  group by p_mfgr
order by p_mfgr;

select calgetstats();
select calflushcache();

-- q0010.2.d.
-- Join Compares: 1.864 Million x 600 Million (5.37s @ 4 PMs)
select p_mfgr, count(*), avg(ps_availqty), avg(p_retailprice),
avg(ps_supplycost) from part, partsupp where p_size in (2, 50) and p_retailprice <
1250  and ps_partkey = p_partkey and ps_suppkey <= 750000 and ps_partkey <= 4000000 and p_partkey <= 4000000  group by p_mfgr
order by p_mfgr;

select calgetstats();
select now();

-- q0010.2.c.
select p_mfgr, count(*), avg(ps_availqty), avg(p_retailprice),
avg(ps_supplycost) from part, partsupp where p_size in (2, 50) and p_retailprice <
1250  and ps_partkey = p_partkey and ps_suppkey <= 750000 and ps_partkey <= 4000000 and p_partkey <= 4000000  group by p_mfgr
order by p_mfgr;

select calgetstats();
select calflushcache();

