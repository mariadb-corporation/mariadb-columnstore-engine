select cidx, CDOUBLE, CAST(CDOUBLE AS DECIMAL(9)) from datatypetestm order by cidx;
drop table if exists bug3798;
create table bug3798 (c1 float) engine=infinidb;
insert into bug3798 values (1234567), (1.2345678);
select c1, cast(c1 as decimal(9,2)), cast(c1 as decimal(14,12)) from bug3798;
drop table bug3798;

