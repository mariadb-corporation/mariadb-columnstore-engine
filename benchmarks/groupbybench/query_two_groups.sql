select count(*) from (select 1 from bench_two_groups GROUP BY c) s;
