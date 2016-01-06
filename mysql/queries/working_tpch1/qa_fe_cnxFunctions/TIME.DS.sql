select cidx, CDATE, TIME(CDATE) from datatypetestm;
select cidx, CDATETIME, TIME(CDATETIME) from datatypetestm;
select cidx, TIME('2009-02-28 22:23:0') from datatypetestm;
select cidx, CDATE from datatypetestm where TIME(CDATE) <> CDATE;
select cidx, CDATETIME from datatypetestm where TIME(CDATETIME) <> CDATETIME;
