select cidx, CDATE, WEEKOFYEAR(CDATE) from datatypetestm;
select cidx, CDATETIME, WEEKOFYEAR(CDATETIME) from datatypetestm;
select cidx, WEEKOFYEAR('2009-02-28') from datatypetestm;
select cidx, WEEKOFYEAR('2009-07-04') from datatypetestm;
select cidx, CDATE from datatypetestm order by WEEKOFYEAR(CDATE), cidx;
select cidx, CDATETIME from datatypetestm order by WEEKOFYEAR(CDATETIME), cidx;
