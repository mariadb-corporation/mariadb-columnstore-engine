drop table if exists bug3565;

-- Create a table with a tinyint.
create table bug3565(c1 tinyint)engine=infinidb;

-- Populate with > 256K rows.
insert into bug3565 (select 0 from orders limit 270000);
alter table bug3565 add column c2 bigint;
alter table bug3565 add column c3 int;

select count(c1) from bug3565;
select count(c2) from bug3565;
select count(c3) from bug3565;

drop table if exists bug3565;
