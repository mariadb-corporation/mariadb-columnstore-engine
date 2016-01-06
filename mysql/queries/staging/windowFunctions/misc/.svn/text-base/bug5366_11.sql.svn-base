select * from region where (r_regionkey+5) in (select min(n_regionkey) over(partition by n_regionkey) + max(n_regionkey) over(partition by n_regionkey) c from nation where r_regionkey=n_regionkey);
