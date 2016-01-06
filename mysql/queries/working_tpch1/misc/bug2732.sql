select n_name from nation where locate('A', n_name, 4) > 0 order by 1;
select n_name from nation where locate('A', n_name) > 0 order by 1;
