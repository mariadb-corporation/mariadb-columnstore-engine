select cidx, CDATE, DAYOFYEAR(CDATE) from datatypetestm;
select cidx, CDATETIME, DAYOFYEAR(CDATETIME) from datatypetestm;
select cidx, DAYOFYEAR('2009-02-28') from datatypetestm;
select cidx, DAYOFYEAR('2009-07-04') from datatypetestm;
select cidx, CDATE from datatypetestm where DAYOFYEAR(CDATE) <> CDATE;
select cidx, CDATETIME from datatypetestm where DAYOFYEAR(CDATETIME) <> CDATETIME;
