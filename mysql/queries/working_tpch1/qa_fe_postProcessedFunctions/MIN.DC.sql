select MIN(CDATE) from datatypetestm;
select MIN(CDATETIME) from datatypetestm;
select cidx, CDATE, MIN(CDATE) from datatypetestm group by cidx, CDATE order by 1;
select cidx, CDATETIME, MIN(CDATETIME) from datatypetestm group by cidx, CDATETIME order by 1;
