-- group by counter
select abs(n_regionkey+n_nationkey), count(*) from nation group by 1 order by 1;
-- group by alias
select abs(n_regionkey+n_nationkey) a, count(*) from nation group by a order by 1;
-- group by FE
select abs(n_regionkey+n_nationkey) a, count(*) from nation group by abs(n_regionkey+n_nationkey) order by 1;
select abs(n_regionkey+n_nationkey) a, count(*) from nation group by 1, abs(n_nationkey+n_regionkey) order by 1;