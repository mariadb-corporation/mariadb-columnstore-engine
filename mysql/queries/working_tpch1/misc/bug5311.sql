select count(*) from sub1 t1 left outer join sub2 t2 on t1.c1 = t2.c1 and t2.c2 < 3;
select count(*) from sub1 t1 left outer join sub2 t2 on t1.c1 = t2.c1 and t2.c2 is null;
select count(*) from sub1 t1 left outer join sub2 t2 on t1.c1 = t2.c1 and t2.c2 is not null;

select count(*) from sub1 t1 left outer join sub2 t2 on t1.c1 = t2.c1 and t2.c2 < 3 and t2.c3 > 1;
select count(*) from sub1 t1 left outer join sub2 t2 on t1.c1 = t2.c1 and t2.c2 is null and t2.c3 > 1;
select count(*) from sub1 t1 left outer join sub2 t2 on t1.c1 = t2.c1 and t2.c2 is not null and t2.c3 > 1;
