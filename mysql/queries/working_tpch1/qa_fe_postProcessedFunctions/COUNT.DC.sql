select COUNT(CDATE) from datatypetestm;
select COUNT(CDATETIME) from datatypetestm;
select cidx, CDATE, COUNT(CDATE) from datatypetestm group by cidx, CDATE order by 1;
select cidx, CDATETIME, COUNT(CDATETIME) from datatypetestm group by cidx, CDATETIME order by 1;
