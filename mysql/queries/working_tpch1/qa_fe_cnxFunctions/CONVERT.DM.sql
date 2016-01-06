select cidx, CDATE, CONVERT(CDATE,DATE) from datatypetestm;
select cidx, CDATE, CONVERT(CDATE,DATETIME) from datatypetestm;
select cidx, CDATETIME, CONVERT(CDATETIME,DATE) from datatypetestm;
select cidx, CDATETIME, CONVERT(CDATETIME,DATETIME) from datatypetestm;
select cidx, CDATE from datatypetestm where CONVERT(CDATE,DATE) <> CDATE;
select cidx, CDATE from datatypetestm where CONVERT(CDATE,DATETIME) <> CDATE;
select cidx, CDATETIME from datatypetestm where CONVERT(CDATETIME,DATE) <> CDATETIME;
select cidx, CDATETIME from datatypetestm where CONVERT(CDATETIME,DATETIME) <> CDATETIME;
