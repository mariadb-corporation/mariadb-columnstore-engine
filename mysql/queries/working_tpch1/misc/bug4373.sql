create table if not exists mkr (c1 int, select_b int) engine=infinidb;
insert into mkr (c1, select_b) (select n_nationkey, n_regionkey from nation);
select * from mkr;
drop table mkr;
