-- Miscellanous queries with distinct.

-- Dummy queries always included.
select now();
select now();
select calflushcache();

-- q0008.1
select count(l_linenumber) from lineitem where l_shipdate <= '1994-03-15' and l_quantity <= 4;
select calgetstats();
select now();

-- q0008.2
select distinct l_linenumber from lineitem where l_shipdate <= '1994-03-15' and l_quantity <= 4 order by 1;
select calgetstats();
select now();

-- q0008.3
select distinct l_linenumber from lineitem where l_shipdate <= '1994-03-15' and l_quantity <= 8 order by 1;
select calgetstats();
select now();

-- q0008.4
select distinct l_linenumber, l_shipmode from lineitem where l_shipdate <= '1992-03-15' and l_quantity <= 8 order by 1, 2;
select calgetstats();
select now();

-- q0008.5
select distinct l_linenumber, l_shipmode from lineitem where l_shipdate <= '1992-03-15' and l_quantity <= 8 order by 1, 2;
select calgetstats();
select now();

-- q0008.6
select l_shipmode, count(distinct l_partkey) from lineitem where l_shipdate <= '1992-03-15' and l_quantity <= 8 group by 1 order by 1;
select calgetstats();
select now();

-- q0008.7
select l_shipdate, count(distinct l_orderkey), count(*) from lineitem where l_shipdate <= '1992-01-31' group by 1 order by 1;
select calgetstats();
select now();
