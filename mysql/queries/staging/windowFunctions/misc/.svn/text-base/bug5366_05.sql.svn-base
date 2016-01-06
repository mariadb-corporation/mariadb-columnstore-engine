select * from region where (r_regionkey+1) in (select min(n_regionkey+1) over() from nation where r_regionkey=n_regionkey);
