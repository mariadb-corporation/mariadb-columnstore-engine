select cidx, CDATE, WEEK(CDATE) from datatypetestm;
select cidx, CDATETIME, WEEK(CDATETIME) from datatypetestm;
select cidx, WEEK('2009-02-28') from datatypetestm;
select cidx, WEEK('2009-07-04') from datatypetestm;
select cidx, CDATE from datatypetestm order by WEEK(CDATE), cidx;
select cidx, CDATETIME from datatypetestm order by WEEK(CDATETIME), cidx;
