create table if not exists casetest (id int, c1 int, c2 int, c3 int) engine = infinidb;
insert into casetest values (1, 1, 1, 1), (2, null, null, null), (3, 1, 1, null), (4, 1, null, null);
select id, case when c1 is not null and c2 is not null and c3 is not null then 'A' when c1 is not null and c2 is 
not null and c3 is null then 'B' when c1 is not null and c2 is null and c3 is not null then 'C' when c1 is not null 
and c2 is null and c3 is null then 'D' end case_func from casetest;
drop table casetest;
