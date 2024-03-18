select count(*) from (select 1 from bench_ten_groups GROUP BY c) s;
