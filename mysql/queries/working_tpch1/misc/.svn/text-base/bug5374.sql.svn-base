select r_name, n_name from region left outer join nation on r_name = n_name where n_name is null;
select n_nationkey, r_regionkey from nation left join region on r_regionkey=n_nationkey where r_regionkey < 2;
select n_nationkey from nation left join region on r_regionkey=n_nationkey where (case when r_regionkey < 2 then 1 else 2 end) < 2;
select n_nationkey, r_regionkey from nation left join region on r_regionkey=n_nationkey where (case when r_regionkey < 2 then 1 else 0 end) > 0;
