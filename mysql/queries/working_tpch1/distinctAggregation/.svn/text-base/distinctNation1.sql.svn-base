select distinct n_regionkey from nation order by 1;
select distinct n_nationkey from nation order by 1;
select avg(distinct n_nationkey) from nation;
select count(distinct n_name) from nation;
select count(distinct n_regionkey) from nation;
select count(distinct n_nationkey), count(distinct n_regionkey) from nation;
select avg(distinct n_nationkey), avg(distinct n_regionkey) from nation;
select count(n_nationkey), count(n_regionkey) from nation;
select count(distinct n_name), avg(distinct n_nationkey) from nation;
select count(distinct n_nationkey), count(distinct n_name), count(distinct n_regionkey), count(distinct n_comment) from nation;
select sum(distinct n_nationkey), count(distinct n_name), avg(n_regionkey), sum(distinct n_regionkey), max(distinct n_comment), max(n_comment) from nation;

select count(distinct n_comment) from nation;

select count(distinct n_nationkey) from nation where n_nationkey < 0;
select count(distinct n_nationkey) from nation where n_nationkey < 10;
select count(distinct n_name) from nation where n_nationkey < 10;
select count(distinct n_regionkey) from nation where n_nationkey < 10;
select count(distinct n_nationkey), count(distinct n_regionkey) from nation where n_nationkey < 10;
select count(distinct n_name), count(distinct n_nationkey) from nation where n_nationkey < 10;
select count(distinct n_nationkey), count(distinct n_name), count(distinct n_regionkey), count(distinct n_comment) from nation where n_nationkey < 10;

