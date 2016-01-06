drop table if exists tbl1;
drop table if exists tbl2;

create table tbl1 (c1 char(1), c2 char(255))engine=infinidb;
create table tbl2 (c1 char(1), c2 char(255))engine=infinidb;

insert into tbl1 values (1, 1), (2, 2), (3, 3), (4, 4), (null, null);
insert into tbl2 values (1, 1), (2, 2), (3, 3), (null, null);

select count(*) from tbl1 join tbl2 on tbl1.c1 = tbl2.c1;
select count(*) from tbl1 join tbl2 on tbl1.c1 = tbl2.c2;
select count(*) from tbl1 join tbl2 on tbl1.c2 = tbl2.c1;
select count(*) from tbl1 join tbl2 on tbl1.c2 = tbl2.c2;

select count(*) from tbl1 left join tbl2 on tbl1.c1 = tbl2.c1;
select count(*) from tbl1 left join tbl2 on tbl1.c1 = tbl2.c2;
select count(*) from tbl1 left join tbl2 on tbl1.c2 = tbl2.c1;
select count(*) from tbl1 left join tbl2 on tbl1.c2 = tbl2.c2;

select count(*) from tbl1 right join tbl2 on tbl1.c1 = tbl2.c1;
select count(*) from tbl1 right join tbl2 on tbl1.c1 = tbl2.c2;
select count(*) from tbl1 right join tbl2 on tbl1.c2 = tbl2.c1;
select count(*) from tbl1 right join tbl2 on tbl1.c2 = tbl2.c2;

drop table tbl1;
drop table tbl2;
