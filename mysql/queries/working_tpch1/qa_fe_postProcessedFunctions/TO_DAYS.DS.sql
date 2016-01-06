select cidx, CDATE, TO_DAYS(CDATE) from datatypetestm;
select cidx, CDATETIME, TO_DAYS(CDATETIME) from datatypetestm;
select cidx, TO_DAYS('2009-02-28') from datatypetestm;
select cidx, TO_DAYS('2009-07-04') from datatypetestm;
select cidx, CDATE from datatypetestm order by TO_DAYS(CDATE), cidx;
select cidx, CDATETIME from datatypetestm order by TO_DAYS(CDATETIME), cidx;
