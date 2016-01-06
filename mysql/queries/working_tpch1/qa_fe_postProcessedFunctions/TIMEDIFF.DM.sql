select cidx, CDATE, TIMEDIFF(CDATE,'2007-02-28 22:23:0') from datatypetestm;
select cidx, CDATETIME, TIMEDIFF(CDATETIME,'2007-02-28 22:23:0') from datatypetestm;
select cidx, CDATE from datatypetestm order by TIMEDIFF(CDATE,'2007-02-28 22:23:0'), cidx;
select cidx, CDATETIME from datatypetestm order by TIMEDIFF(CDATETIME,'2007-02-28 22:23:0'), cidx;
