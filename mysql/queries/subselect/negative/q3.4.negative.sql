select * from sub1 where c1 <= (select c3 from sub2 where c1 = 1) order by 1;

