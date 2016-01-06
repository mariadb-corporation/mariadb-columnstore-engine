select cidx, CDATE, DAYOFMONTH(CDATE) from datatypetestm;
select cidx, CDATETIME, DAYOFMONTH(CDATETIME) from datatypetestm;
select cidx, DAYOFMONTH('2009-02-28') from datatypetestm;
select cidx, DAYOFMONTH('2009-07-04') from datatypetestm;
select cidx, CDATE from datatypetestm order by DAYOFMONTH(CDATE), cidx;
select cidx, CDATETIME from datatypetestm order by DAYOFMONTH(CDATETIME), cidx;
