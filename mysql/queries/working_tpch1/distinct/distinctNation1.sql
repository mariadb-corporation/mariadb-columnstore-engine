select distinct * from nation order by 1,2,3,4;
select distinct n_nationkey from nation order by 1;
select distinct n_name from nation order by 1;
select distinct n_regionkey from nation order by 1;
select distinct n_nationkey, n_regionkey from nation order by 1, 2;
select distinct n_name, n_nationkey from nation order by 1,2;
select distinct n_nationkey, n_name, n_regionkey, n_comment from nation order by 1,2;
select distinct n_comment from nation order by 1;

select distinct * from nation where n_nationkey < 10 order by n_nationkey;
select distinct n_nationkey from nation where n_nationkey < 10 order by 1;
select distinct n_name from nation where n_nationkey < 10 order by 1;
select distinct n_regionkey from nation where n_nationkey < 10 order by 1;
select distinct n_nationkey, n_regionkey from nation where n_nationkey < 10 order by 1,2;
select distinct n_name, n_nationkey from nation where n_nationkey < 10 order by 1,2;
select distinct n_nationkey, n_name, n_regionkey, n_comment from nation where n_nationkey < 10 order by 1;
select distinct n_comment from nation where n_nationkey < 10 order by 1;

select distinct * from nation where n_nationkey > 10 order by 1;
select distinct n_nationkey from nation where n_nationkey > 10 order by 1;
select distinct n_name from nation where n_nationkey > 10 order by 1;
select distinct n_regionkey from nation where n_nationkey > 10 order by 1;
select distinct n_nationkey, n_regionkey from nation where n_nationkey > 10 order by 1,2;
select distinct n_name, n_nationkey from nation where n_nationkey > 10 order by 1,2;
select distinct n_nationkey, n_name, n_regionkey, n_comment from nation where n_nationkey > 10 order by 1,3;
select distinct n_comment from nation where n_nationkey > 10 order by 1;

