select avg(if(n_regionkey>0, n_nationkey, NULL)) from nation;
select r_regionkey as r, NULL as n from region union select r_regionkey, r_name from region order by 1, 2;

