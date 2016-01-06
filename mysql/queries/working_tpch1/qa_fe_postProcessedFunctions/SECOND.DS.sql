select cidx, CDATE, SECOND(CDATE) from datatypetestm;
select cidx, CDATETIME, SECOND(CDATETIME) from datatypetestm;
select cidx, SECOND('1:14:25') from datatypetestm;
select cidx, SECOND('13:14:25') from datatypetestm;
select cidx, CDATE from datatypetestm order by SECOND(CDATE), cidx;
select cidx, CDATETIME from datatypetestm order by SECOND(CDATETIME), cidx;
