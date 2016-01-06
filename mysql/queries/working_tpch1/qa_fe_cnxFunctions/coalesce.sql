select c1,c2,c3,coalesce(c2,c3,s2,s1,s3) from sub1;
select min(coalesce(c2,c3,s1,s2,s1,s3)) from sub1 group by c1 order by c1;
select c1, c2, c3 from sub1 where coalesce(c2,c3) is not null;
select c1, c2, c3 from sub1 order by coalesce(null,s1,c2,c3);
select coalesce(cvchar255,cvchar8)from datatypetestm;
select cidx, CDECIMAL4_2, COALESCE(CDECIMAL4_2,1,NULL,NULL,NULL,2,NULL) from datatypetestm order by cidx;
select cidx, CDATETIME, COALESCE(CDATETIME,NULL) from datatypetestm order by cidx;
