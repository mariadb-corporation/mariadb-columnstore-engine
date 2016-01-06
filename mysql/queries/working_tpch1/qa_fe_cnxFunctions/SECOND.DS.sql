select cidx, CDATE, SECOND(CDATE) from datatypetestm;
select cidx, CDATETIME, SECOND(CDATETIME) from datatypetestm;
select cidx, SECOND('1:14:25') from datatypetestm;
select cidx, SECOND('13:14:25') from datatypetestm;
select cidx, CDATE from datatypetestm where SECOND(CDATE) <> CDATE;
select cidx, CDATETIME from datatypetestm where SECOND(CDATETIME) <> CDATETIME;
