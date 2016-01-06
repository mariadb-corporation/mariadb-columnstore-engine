select * from sub1 where exists (select * from sub2 where sub1.c1 = sub2.c1 and sub2.c2 > 1) order by 1;
select * from sub1 where not exists (select * from sub5 where sub1.c1 = sub5.c1 and sub5.c2 > 1) order by 1, 2, 3, 4, 5, 6;
select * from sub1 where c1 in (select c1 from sub2 where sub1.c1 < sub2.c3) order by 1;
select * from sub1 where c1 in (select c1 from sub2 where sub1.c3 = sub2.c3) order by 1;
select * from sub1 where c1 not in (select c1 from sub2 where sub1.c3 = sub2.c3) order by 1;
select * from sub1 where c1 not in (select c1 from sub2 where sub1.c3 < sub2.c3) order by 1;
select * from sub1, sub3 where sub1.c1 in (select c1 from sub2 where sub1.c2 >= sub2.c3) and sub1.c2 = sub3.c2 order by 1;
