select * from sub1 left outer join (select * from sub2)a on (sub1.c1=a.c1 and sub1.c2=a.c2) order by 1,2, 3, 4, 5, 6, 7, 8, 9, 10;
select * from sub1 right outer join (select * from sub2)a on (sub1.c1=a.c1 and sub1.c2=a.c2) order by 1,2, 3, 4, 5, 6, 7, 8, 9, 10;
select * from sub1 left outer join (select * from sub2)a using(c1,c2) order by 1,2, 3, 4, 5, 6, 7, 8, 9, 10;
select * from sub1 right outer join (select * from sub2)a using(c1,c2) order by 1,2, 3, 4, 5, 6, 7, 8, 9, 10;
