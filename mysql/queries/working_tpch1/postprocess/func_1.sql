select abs(n_regionkey), format(n_regionkey, 2), greatest(n_regionkey,n_nationkey), least(n_regionkey, n_nationkey), truncate(n_regionkey,-1) from nation order by n_regionkey, n_nationkey;
select abs(r_regionkey), r_regionkey from region;

