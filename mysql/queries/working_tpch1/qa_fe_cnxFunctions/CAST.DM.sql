select cidx, CDATE, CAST(CDATE AS DATE) from datatypetestm;
select cidx, CDATE, CAST(CDATE AS DATETIME) from datatypetestm;
select cidx, CDATETIME, CAST(CDATETIME AS DATE) from datatypetestm;
select cidx, CDATETIME, CAST(CDATETIME AS DATETIME) from datatypetestm;
select cidx, CDATE from datatypetestm where CAST(CDATE AS DATE) <> CDATE;
select cidx, CDATE from datatypetestm where CAST(CDATE AS DATETIME) <> CDATE;
select cidx, CDATETIME from datatypetestm where CAST(CDATETIME AS DATE) <> CDATETIME;
select cidx, CDATETIME from datatypetestm where CAST(CDATETIME AS DATETIME) <> CDATETIME;
