select cidx, CDATE, WEEKDAY(CDATE) from datatypetestm;
select cidx, CDATETIME, WEEKDAY(CDATETIME) from datatypetestm;
select cidx, WEEKDAY('2009-02-28') from datatypetestm;
select cidx, WEEKDAY('2009-07-04') from datatypetestm;
select cidx, CDATE from datatypetestm order by WEEKDAY(CDATE), cidx;
select cidx, CDATETIME from datatypetestm order by WEEKDAY(CDATETIME), cidx;
