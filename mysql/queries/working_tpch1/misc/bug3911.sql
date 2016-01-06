select count(n_nationkey) from nation n join region r on n_regionkey = r_regionkey and n_nationkey in (select distinct n_regionkey from nation where n.n_nationkey < 10);

select count(n_nationkey) from nation n join region r on n_regionkey = r_regionkey and n_nationkey in (select distinct n_regionkey from nation where n.n_name <> 'abc');

