select cdatetime, year(cdatetime), week(cdatetime) from datatypetestm group by 1, 2, 3 order by 1;
select cdatetime, week(cdatetime) from datatypetestm group by 1, 2 order by 1;
select cdatetime, year(cdatetime) from datatypetestm group by 1, 2 order by 1;

