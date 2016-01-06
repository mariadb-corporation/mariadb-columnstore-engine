-- From Jim's Bench_HJ_Part_PartSupp.sql script.

/*
mysql> desc part
    -> ;
+---------------+---------------+------+-----+---------+-------+
| Field         | Type          | Null | Key | Default | Extra |
+---------------+---------------+------+-----+---------+-------+
| p_partkey     | int(11)       | YES  |     | NULL    |       |
| p_name        | varchar(55)   | YES  |     | NULL    |       |
| p_mfgr        | char(25)      | YES  |     | NULL    |       |
| p_brand       | char(10)      | YES  |     | NULL    |       |
| p_type        | varchar(25)   | YES  |     | NULL    |       |
| p_size        | int(11)       | YES  |     | NULL    |       |
| p_container   | char(10)      | YES  |     | NULL    |       |
| p_retailprice | decimal(12,2) | YES  |     | NULL    |       |
| p_comment     | varchar(23)   | YES  |     | NULL    |       |
+---------------+---------------+------+-----+---------+-------+
9 rows in set (0.00 sec)

mysql> desc partsupp;
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

-- -------------------------------------------------------------------------
-- Verify count at 1,000,000 for small side of join
-- -------------------------------------------------------------------------
--  select count(*) from part where p_size = 50 and p_retailprice < 1250;
-- -------------------------------------------------------------------------

select now();
select now();
select calflushcache();
-- -------------------------------------------------------------------------
-- q0004.1.d
-- select count(*) '~200 Million Parts' from partsupp where ps_suppkey <= 2500000;

select p_mfgr, count(*), avg(ps_availqty), avg(p_retailprice), avg(ps_supplycost) 
from part, partsupp 
where p_size = 50 and p_retailprice < 1250 
	and ps_partkey = p_partkey 
and ps_suppkey <= 250000
and ps_partkey <= 4000000
and p_partkey <= 4000000
group by p_mfgr
order by p_mfgr;

select calgetstats();
select now();

-- q0004.1.c
select p_mfgr, count(*), avg(ps_availqty), avg(p_retailprice), avg(ps_supplycost) 
from part, partsupp 
where p_size = 50 and p_retailprice < 1250 
	and ps_partkey = p_partkey 
and ps_suppkey <= 250000
and ps_partkey <= 4000000
and p_partkey <= 4000000
group by p_mfgr
order by p_mfgr;

select calgetstats();
select calflushcache();

-- -------------------------------------------------------------------------
-- q0004.2.d
-- select count(*) '~400 Million Parts' from partsupp where ps_suppkey <= 5000000;

select p_mfgr, count(*), avg(ps_availqty), avg(p_retailprice), avg(ps_supplycost) 
from part, partsupp 
where p_size = 50 and p_retailprice < 1250 
	and ps_partkey = p_partkey 
and ps_suppkey <= 500000
and ps_partkey <= 4000000
and p_partkey <= 4000000
group by p_mfgr
order by p_mfgr;

select calgetstats();
select now();

-- q0004.2.c
select p_mfgr, count(*), avg(ps_availqty), avg(p_retailprice), avg(ps_supplycost) 
from part, partsupp 
where p_size = 50 and p_retailprice < 1250 
	and ps_partkey = p_partkey 
and ps_suppkey <= 500000
and ps_partkey <= 4000000
and p_partkey <= 4000000
group by p_mfgr
order by p_mfgr;

select calgetstats();
select calflushcache();

-- -------------------------------------------------------------------------
-- q0004.3.d
-- select count(*) '~600 Million Parts' from partsupp where ps_suppkey <= 7500000;

select p_mfgr, count(*), avg(ps_availqty), avg(p_retailprice), avg(ps_supplycost) 
from part, partsupp 
where p_size = 50 and p_retailprice < 1250 
	and ps_partkey = p_partkey 
and ps_suppkey <= 750000
and ps_partkey <= 4000000
and p_partkey <= 4000000
group by p_mfgr
order by p_mfgr;

select calgetstats();
select now();

-- q0004.3.c
select p_mfgr, count(*), avg(ps_availqty), avg(p_retailprice), avg(ps_supplycost) 
from part, partsupp 
where p_size = 50 and p_retailprice < 1250 
	and ps_partkey = p_partkey 
and ps_suppkey <= 750000
and ps_partkey <= 4000000
and p_partkey <= 4000000
group by p_mfgr
order by p_mfgr;

select calgetstats();
select calflushcache();

-- -------------------------------------------------------------------------
-- q0004.4.d
-- select count(*) '~800 Million Parts' from partsupp where ps_suppkey <= 10000000;

select p_mfgr, count(*), avg(ps_availqty), avg(p_retailprice), avg(ps_supplycost) 
from part, partsupp 
where p_size = 50 and p_retailprice < 1250 
	and ps_partkey = p_partkey 
and ps_suppkey <= 1000000
and ps_partkey <= 4000000
and p_partkey <= 4000000
group by p_mfgr
order by p_mfgr;

select calgetstats();
select now();

-- q0004.4.c
select p_mfgr, count(*), avg(ps_availqty), avg(p_retailprice), avg(ps_supplycost) 
from part, partsupp 
where p_size = 50 and p_retailprice < 1250 
	and ps_partkey = p_partkey 
and ps_suppkey <= 1000000
and ps_partkey <= 4000000
and p_partkey <= 4000000
group by p_mfgr
order by p_mfgr;

select calgetstats();
select now();
-- -------------------------------------------------------------------------
