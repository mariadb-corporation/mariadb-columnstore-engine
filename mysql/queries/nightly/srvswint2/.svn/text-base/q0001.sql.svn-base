-- From Jim's bench_dbt3_aggregation.sql script.

-- Call getstats() twice below before cache queries so that every third query is the "real"
-- one for parsing out the times.

select now();
select now();
select calflushcache();

-- q0001.1.d
select l_discount, count(*) from lineitem where l_shipdate between '1994-12-01' and '1995-02-28'
     group by 1 order by 1;
select calgetstats();
select now();

-- q0001.1.c
select l_discount, count(*) from lineitem where l_shipdate between '1994-12-01' and '1995-02-28'
     group by 1 order by 1;
select calgetstats();
select calflushcache();

-- q0001.2.d
select l_returnflag,l_discount,l_linenumber, count(*) from lineitem where l_shipdate between '1994-12-01' and '1995-01-28'
     group by 1,2,3 order by 1,2,3;
select calgetstats();
select now();

-- q0001.2.c
select l_returnflag,l_discount,l_linenumber, count(*) from lineitem where l_shipdate between '1994-12-01' and '1995-01-28'
     group by 1,2,3 order by 1,2,3;
select calgetstats();
select calflushcache();

-- q0001.3.d
select l_returnflag,l_discount,l_linenumber,l_linestatus,l_tax, count(*) from lineitem where l_shipdate between '1994-12-01' and '1995-01-28'
     group by 1,2,3,4,5 order by 1,2,3,4,5;
select calgetstats();
select now();

-- q0001.3.c
select l_returnflag,l_discount,l_linenumber,l_linestatus,l_tax, count(*) from lineitem where l_shipdate between '1994-12-01' and '1995-01-28'
     group by 1,2,3,4,5 order by 1,2,3,4,5;
select calgetstats();
select now();
