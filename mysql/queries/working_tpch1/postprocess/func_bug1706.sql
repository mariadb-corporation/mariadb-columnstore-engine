select substr(na.n_name, 1, 2) from nation na order by n_nationkey;
select concat(na.n_name, ';') from nation na order by n_nationkey;
select length(na.n_name) from nation na order by n_nationkey;
select substr(length(na.n_name), 1, 1) from nation na order by n_nationkey;
select na.n_nationkey, least(12, na.n_nationkey) from nation na order by n_nationkey;
select na.n_nationkey, na.n_name from nation na order by substr(na.n_name, 1, 2) desc, n_nationkey;
select n_name, SUBSTR(n_name from 4) from nation order by n_nationkey;
select n_name, SUBSTRING(n_name from 4) from nation order by n_nationkey;
select n_name, SUBSTRING(n_name from 4 for 3) from nation order by n_nationkey;
select n_nationkey, SUBSTRING('Kalakawa' FROM 4) from nation order by n_nationkey;
select n_nationkey, SUBSTRING('Kalakawa' FROM 4 FOR 3) from nation order by n_nationkey;


