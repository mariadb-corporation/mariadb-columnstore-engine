select from_days(c2+730669), from_days(c2), from_days(c2 - 730669) from sub1 where from_days(c2+730669) > '2000-07-05';
select cinteger, from_days(cinteger/10), from_days(730669) from datatypetestm;
select cidx, CDECIMAL9_2, FROM_DAYS(CDECIMAL9_2) from datatypetestm order by cidx;
