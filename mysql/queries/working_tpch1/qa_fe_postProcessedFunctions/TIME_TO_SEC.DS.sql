select cidx, CDATE, TIME_TO_SEC(CDATE) from datatypetestm;
select cidx, CDATETIME, TIME_TO_SEC(CDATETIME) from datatypetestm;
select cidx, TIME_TO_SEC('1:14:25') from datatypetestm;
select cidx, TIME_TO_SEC('13:14:25') from datatypetestm;
select cidx, CDATE from datatypetestm order by TIME_TO_SEC(CDATE), cidx;
select cidx, CDATETIME from datatypetestm order by TIME_TO_SEC(CDATETIME), cidx;
