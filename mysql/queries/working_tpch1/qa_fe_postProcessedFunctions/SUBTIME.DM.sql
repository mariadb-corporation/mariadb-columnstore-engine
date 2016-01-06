select cidx, CDATE, SUBTIME(CDATE,'1:14:25') from datatypetestm;
select cidx, CDATE, SUBTIME(CDATE,'13:14:25') from datatypetestm;
select cidx, CDATETIME, SUBTIME(CDATETIME,'1:14:25') from datatypetestm;
select cidx, CDATETIME, SUBTIME(CDATETIME,'13:14:25') from datatypetestm;
select cidx, CDATE from datatypetestm order by SUBTIME(CDATE,'1:14:25'), cidx;
select cidx, CDATE from datatypetestm order by SUBTIME(CDATE,'13:14:25'), cidx;
select cidx, CDATETIME from datatypetestm order by SUBTIME(CDATETIME,'1:14:25'), cidx;
select cidx, CDATETIME from datatypetestm order by SUBTIME(CDATETIME,'13:14:25'), cidx;
