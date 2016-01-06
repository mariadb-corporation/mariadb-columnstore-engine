# WWW - Edits to cut down number of rows and added parenthes to not have ors at the same level as joins.
# one table filters
#
select c_custkey,c_name from customer where c_custkey < 1000 and c_name like '%500' order by c_custkey;
select c_custkey,c_name from customer where c_custkey < 1000 and (c_name like '%005' or c_name like '%004') order by c_custkey;
select c_custkey,c_name from customer where c_custkey < 1000 and c_name like '%500' or c_name like '%400' order by c_custkey;
select c_custkey,c_name from customer where c_custkey < 1000 and (c_name like '%500' or c_name like '%400') order by c_custkey;
select c_custkey,c_name from customer where c_custkey < 10 or c_name like '%500' order by c_custkey;
select c_custkey,c_name from customer where c_custkey < 40 or c_name like '%5000' or c_name like '%2010' order by c_custkey;
select c_custkey,c_name from customer where (c_custkey < 10 or c_name like '%500') or c_name like '%400' order by c_custkey;
select c_custkey,c_name from customer where c_custkey < 44 or (c_name like '%500' or c_name like '%400') order by c_custkey;
select c_custkey,c_name from customer where (c_custkey < 1000 and c_name like '%500') or c_name like '%400' order by c_custkey;
select c_custkey,c_name from customer where c_custkey < 1000 and (c_name like '%500' or c_name like '%400') order by c_custkey;
select c_custkey,c_name,c_nationkey from customer where (c_custkey < 1000 or c_name like '%500') or c_name like '%400' and c_nationkey = 1 order by c_custkey;
select c_custkey,c_name,c_nationkey from customer where (c_custkey < 1000 or c_name like '%500') and c_name like '%400' and c_nationkey = 1 order by c_custkey;
select c_custkey,c_name,c_nationkey from customer where ((c_custkey < 1000 or c_name like '%500') or c_name like '%400') and c_nationkey = 1 order by c_custkey;
select c_custkey,c_name,c_nationkey from customer where ((c_custkey < 1000 and c_name like '%500') or c_name like '%400') and c_nationkey = 1 order by c_custkey;
select c_custkey,c_name,c_nationkey from customer where ((c_custkey < 1000 and c_name like '%5020') or c_name like '%4100') or (c_nationkey = 1 and c_custkey < 888) order by c_custkey;
select c_custkey,c_name,c_nationkey from customer where (c_custkey < 1000 and c_name like '%500') or (c_name like '%400' or (c_custkey < 2000 and c_nationkey = 1)) order by c_custkey;
select c_custkey,c_name,c_nationkey from customer where (c_custkey < 1000 and c_name like '%500') or (c_name like '%400' and c_nationkey = 1) order by c_custkey;
select c_custkey,c_name,c_nationkey from customer where (c_custkey < 1000 and c_name like '%1') and (c_name like '%400' and c_nationkey = 1) order by c_custkey;
select c_custkey,c_name,c_nationkey from customer where (c_custkey < 1000 and c_name like '%500') or (c_acctbal < 0 and c_nationkey = 1) order by c_custkey;
select c_custkey,c_name,c_nationkey from customer where (c_custkey < 1000 and c_name like '%500') or (c_acctbal < 0 and c_nationkey = 1 or (c_name like '%2000' and c_nationkey=2) ) order by c_custkey;
select c_custkey,c_name,c_nationkey from customer where (c_custkey < 1000 or c_name like '%50000') or (c_acctbal < 0 and c_nationkey = 1 or c_nationkey=2 ) order by c_custkey;
select c_custkey,c_name,c_nationkey from customer where (c_custkey < 100 or c_name like '%50000') or ((c_acctbal < -7000 and c_nationkey = 1) or (c_nationkey=2 and c_acctbal > 9500.0)) order by c_custkey;
select c_custkey,c_name,c_nationkey from customer where ((c_custkey < 100 or c_name like '%50000') or (c_acctbal < -7000 and c_nationkey = 1)) or (c_nationkey=2 and c_acctbal > 9500.0) order by c_custkey;
select c_custkey,c_name,c_nationkey from customer where ((c_custkey < 100 and c_name like '%50000') or (c_acctbal < -1000 and c_nationkey = 1)) or (c_nationkey=2 and c_acctbal > 9500.0) order by c_custkey;
select c_custkey,c_name,c_nationkey from customer where (c_custkey < 100 and (c_name like '%50000' or c_acctbal < -1000) and c_nationkey = 1) or (c_nationkey=2 and c_acctbal > 9500.0) order by c_custkey;
select c_custkey,c_name,c_nationkey from customer where (c_custkey < 100 and (c_name like '%50000' or c_acctbal < -1000 and c_nationkey = 1)) or (c_nationkey=2 and c_acctbal > 9500.0) order by c_custkey;
#
# Two table join and filters
#
select c_custkey, n_name from customer c, nation n where c_nationkey = n_nationkey and c_custkey < 100 order by c_custkey;
select c_custkey, n_name from customer c, nation n where c_nationkey = n_nationkey and c_custkey < 1000 and (c_nationkey = 1 or n_nationkey = 2) order by c_custkey;
select c_custkey, n_name from customer c, nation n where (c_nationkey = 1 or n_nationkey = 3) and c_nationkey = n_nationkey and c_custkey < 487 order by c_custkey;
select c_custkey, n_name from customer c, nation n where (c_nationkey = 1 or n_nationkey = 2) and c_nationkey = n_nationkey and c_custkey < 1000 order by c_custkey;
select c_custkey, n_name from customer c, nation n where c_nationkey = n_nationkey and c_custkey < 1000 and (c_nationkey = 1 or c_name like '%99%') order by c_custkey;
select c_custkey, n_name from customer c, nation n where c_nationkey = n_nationkey and c_custkey < 1000 and (c_nationkey = 1 or n_nationkey = 2) order by c_custkey;
select c_custkey, n_name from customer c, nation n where c_nationkey = n_nationkey and (c_custkey < 1500 and (c_nationkey = 1 or n_nationkey = 2)) order by c_custkey;
select c_custkey, n_name from customer c, nation n where c_nationkey = n_nationkey and (c_custkey < 2000 and (c_nationkey = 1 or n_nationkey = 2)) order by c_custkey;
select c_custkey, n_name from customer c, nation n where c_nationkey = n_nationkey and (c_custkey < 1000 and (((c_nationkey = 1 or n_nationkey = 2)))) and n_regionkey = 1 order by c_custkey;
select c_custkey, n_name from customer c, nation n where c_nationkey = n_nationkey and (c_name like '%1000%' and (c_nationkey = 1 or n_nationkey = 4 or n_regionkey = 1)) order by c_custkey;
select c_custkey, n_name from customer c, nation n where c_nationkey = n_nationkey and c_custkey < 1000 and (c_nationkey = 1 or n_nationkey = 2) and n_regionkey = 1 order by c_custkey;
select c_custkey, n_name from customer c, nation n where c_nationkey = n_nationkey and c_custkey < 1000 and (c_nationkey = 1 or n_nationkey = 2 or n_regionkey = 1) order by c_custkey;
select c_custkey, n_name from customer c, nation n where (c_nationkey = n_nationkey) and (c_nationkey <> n_regionkey and c_custkey < 1000) order by c_custkey;
select c_custkey, n_name from customer c, nation n where (c_nationkey = n_nationkey) and (c_nationkey <> c_custkey) and c_custkey < 300 order by c_custkey;
#
# three table join and filters
#
select c_custkey, n_name, r_name from customer c, nation n, region r where c_nationkey = n_nationkey and n_regionkey = r_regionkey and c_custkey < 1000 order by c_custkey;
select c_custkey, n_name, r_name from customer c, nation n, region r where c_nationkey = n_nationkey and n_regionkey = r_regionkey and c_custkey < 1000 and r_regionkey = 1 order by c_custkey;
select c_custkey, n_name, r_name from customer c, nation n, region r where c_nationkey = n_nationkey and n_regionkey = r_regionkey and c_custkey < 1000 and r_regionkey = 1 and n_name like 'UNITED%' order by c_custkey;
select c_custkey, c_name, n_name, r_name from customer c, nation n, region r where c_nationkey = n_nationkey and n_regionkey = r_regionkey and c_custkey < 1000 and r_regionkey = 1 and (n_name like 'UNITED%' or n_name like 'CA%' and c_name like '%50') order by c_custkey;
select c_custkey, c_name, n_name, r_name from customer c, nation n, region r where c_nationkey = n_nationkey and n_regionkey = r_regionkey and c_custkey < 1000 and r_regionkey = 1 and (n_name like 'UNITED%' or (n_name like 'CA%' and c_name like '%50')) order by c_custkey;
select c_custkey, c_name, n_name, r_name from customer c, nation n, region r where c_nationkey = n_nationkey and n_regionkey = r_regionkey and c_custkey < 1000 and (r_regionkey = 1 or (n_name like 'UNITED%' or (n_name like 'CA%' and c_name like '%50'))) order by c_custkey;
select c_custkey, c_name, n_name, r_name from customer c, nation n, region r where c_nationkey = n_nationkey and n_regionkey = r_regionkey and c_custkey < 1000 and (r_regionkey = 1 and (n_name like 'UNITED%' and r_name='ASIA') or (n_name like 'CA%' or c_name like '%50')) order by c_custkey;
select c_custkey, c_name, n_name, r_name from customer c, nation n, region r where c_nationkey = n_nationkey and n_regionkey = r_regionkey and c_custkey < 1000 and (r_regionkey = 1 and (r_name='ASIA' or n_name like 'UNITED%' ) or (n_name like 'CA%' and c_name like '%50')) order by c_custkey;
select c_custkey, c_name, n_name, r_name from customer c, nation n, region r where c_nationkey = n_nationkey and n_regionkey = r_regionkey and c_custkey < 1000 and (r_regionkey = 1 or n_name like 'UNITED%' ) and (c_name like '%50') order by c_custkey;
select c_custkey, c_name, n_name, r_name from customer c, nation n, region r where c_nationkey = n_nationkey and n_regionkey = r_regionkey and c_custkey < 1000 and ((r_regionkey = 1 or n_name like 'UNITED%' ) or (c_name like '%50000' and n_name ='PERU')) order by c_custkey;
select c_custkey, c_name, n_name, r_name from customer c, nation n, region r where c_nationkey = n_nationkey and n_regionkey = r_regionkey and c_custkey < 1000 and (r_regionkey = 1 or n_name like 'UNITED%' ) and (c_name like '%5') order by c_custkey;
select c_custkey, c_name, n_name, r_name from customer c, nation n, region r where c_nationkey = n_nationkey and n_regionkey = r_regionkey and c_custkey < 1000 and (r_regionkey = 1 or n_name like 'UNITED%' ) and (c_name like '%5' and n_name = 'PERU') order by c_custkey;
select c_custkey, c_name, n_name, r_name from customer c, nation n, region r where c_nationkey = n_nationkey and n_regionkey = r_regionkey and c_custkey < 1000 and (r_regionkey = 1 or n_name like 'UNITED%' ) and (c_name like '%5' and n_name = 'PERU' and c_custkey = 775) order by c_custkey;
select c_custkey, c_name, n_name, r_name from customer c, nation n, region r where c_nationkey = n_nationkey and n_regionkey = r_regionkey and c_custkey < 1000 and ((r_regionkey = 1 or n_name like 'UNITED%' ) and (c_name like '%5' and n_name = 'PERU' and c_custkey = 775) or c_custkey = 999) order by c_custkey;
select c_custkey, c_name, n_name, r_name from customer c, nation n, region r where c_nationkey = n_nationkey and n_regionkey = r_regionkey and c_custkey < 1000 and ((r_regionkey = 1 or n_name like 'UNITED%' ) and (c_name like '%5' and n_name = 'PERU' and c_custkey = 775) or c_custkey = 1001) order by c_custkey;
select c_custkey, c_name, n_name, r_name from customer c, nation n, region r where c_nationkey = n_nationkey and n_regionkey = r_regionkey and c_custkey < 1000 and ((r_regionkey = 1 or n_name like 'UNITED%' ) and ((c_name like '%5' or n_name = 'PERU') and c_custkey = 775) or c_custkey = 1001) order by c_custkey;

