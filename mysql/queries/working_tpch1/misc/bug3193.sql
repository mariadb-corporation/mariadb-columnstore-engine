select * from sub1 LEFT OUTER JOIN (select * from sub2) Meal on(sub1.c1=Meal.c1 and sub1.c2=Meal.c2) order by 1;
