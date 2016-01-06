select cidx, CDATE, HOUR(CDATE) from datatypetestm;
select cidx, CDATETIME, HOUR(CDATETIME) from datatypetestm;
select cidx, HOUR('1:14:25') from datatypetestm;
select cidx, HOUR('13:14:25') from datatypetestm;
select cidx, CDATE from datatypetestm where HOUR(CDATE) <> CDATE;
select cidx, CDATETIME from datatypetestm where HOUR(CDATETIME) <> CDATETIME;
