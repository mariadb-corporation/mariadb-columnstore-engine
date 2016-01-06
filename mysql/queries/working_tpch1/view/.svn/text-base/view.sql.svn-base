-- Basic syntax
create or replace view v_region as select * from region;
select r_regionkey from v_region;
select * from v_region where r_regionkey < 3 order by 1;
alter view v_region as (select r_regionkey, r_name from region where r_regionkey > 2);
select * from v_region where r_regionkey < 3 order by 1;
drop view v_region;

--	VIEW used in joins
create or replace view v_region as select * from region;
select r_regionkey,n_regionkey,n_name from v_region, nation where r_regionkey = n_regionkey and n_nationkey < 10 order by 1,2,3;
create or replace view v_nation as select * from nation;
select r_regionkey,n_regionkey,r_name from v_region left join v_nation on r_regionkey=n_regionkey order by 1,2,3;
drop view v_region, v_nation;

--	VIEW body contains joins
create or replace view v1 as (select sub1.c1 s1c1, sub1.c2 s1c2, sub2.c1 s2c1, sub2.c2 s2c2 from sub1 join sub2 on sub1.c1 = sub2.c1);
create or replace view v2 as (select sub3.c1 s3c1, sub4.c1 s4c1, sub3.c2 s3c2, sub4.c2 s4c2 from sub3 join sub4 on sub3.c1 = sub4.c1);
select * from v1, v2 where v1.s1c1 = v2.s3c1 order by 1,2,3,4,5,6,7,8;
alter view v1 as (select sub1.c1 s1c1, sub1.c2 s1c2, sub2.c1 s2c1, sub2.c2 s2c2 from sub1 left join sub2 on sub1.c1 = sub2.c1);
alter view v2 as (select sub3.c1 s3c1, sub4.c1 s4c1, sub3.c2 s3c2, sub4.c2 s4c2 from sub3 right join sub4 on sub3.c1 = sub4.c1);
select * from v1, v2 where v1.s1c1 = v2.s3c1 order by 1,2,3,4,5,6,7,8;
drop view v1;
drop view v2;

--	VIEW used in subquery
create or replace view v1 as (select sub1.c1 s1c1, sub1.c2 s1c2, sub2.c1 s2c1, sub2.c2 s2c2 from sub1 left join sub2 on sub1.c1 = sub2.c1);
create or replace view v2 as (select sub3.c1 s3c1, sub4.c1 s4c1, sub3.c2 s3c2, sub4.c2 s4c2 from sub3 right join sub4 on sub3.c1 = sub4.c1);
select * from v2 where s3c1 in (select s1c1 from v1) order by 1,2,3,4;
select s1c1, s2c1 from v1 where s1c1 = (select min(s3c1) from v2) order by 1,2;
drop view v1;
drop view v2;

--	VIEW contains WHERE clause subquery
create or replace view v1 as (select * from region where r_regionkey in (select n_regionkey from nation));
select count(*) from v1, nation where v1.r_regionkey=n_regionkey;
drop view v1;

--	VIEW contains SELECT clause subquery
create or replace view v1 as select (select c_name   from customer  join nation on c_nationkey = n_nationkey   join region on n_regionkey = r_regionkey where r_name in ('ASIA') and c_custkey = o_custkey and c_mktsegment='MACHINERY' and c_acctbal <= 30) eurasia, count(*) from orders group by 1 order by 1;
select Eurasia from v1 order by 1;
select count(Eurasia) from v1;
drop view v1;

--	VIEW contains Group By, Aggregation, Distinct, or Having clause
create or replace view v1 as select count(distinct c_custkey) from customer group by c_nationkey;
select * from v1 order by 1;
create or replace view v2 as select distinct n_nationkey from nation where n_regionkey = 1;
select * from v2 order by 1;
drop view v1;
drop view v2;


