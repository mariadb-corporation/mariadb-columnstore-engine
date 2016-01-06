select * from region where (r_regionkey+1) in (select (min(n_regionkey) over() + 1) from nation where r_regionkey=n_regionkey);
