select cidx, CDATETIME, TIME_TO_SEC(CDATETIME) from datatypetestm;
select cidx, TIME_TO_SEC('22:23:00') from datatypetestm;
select cidx, TIME_TO_SEC('00:39:38') from datatypetestm;
select cidx, CDATETIME from datatypetestm where TIME_TO_SEC(CDATETIME) <> CDATETIME;
