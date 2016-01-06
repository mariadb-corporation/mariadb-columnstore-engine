select MAX(CDATE) from datatypetestm;
select MAX(CDATETIME) from datatypetestm;
select cidx, CDATE, MAX(CDATE) from datatypetestm group by cidx, CDATE order by 1;
select cidx, CDATETIME, MAX(CDATETIME) from datatypetestm group by cidx, CDATETIME order by 1;
