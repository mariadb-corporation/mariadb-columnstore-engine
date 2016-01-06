select cdate, cdatetime, TIMEDIFF(CDATETIME,'2007-02-28 22:23:0') from datatypetestm where TIMEDIFF(CDATETIME,'2007-02-28 22:23:0') > 0;
select cdate, cdatetime, TIMEDIFF(CDATETIME,'2007-02-28 22:23:0') from datatypetestm where TIMEDIFF(CDATETIME,'2007-02-28 22:23:0') = 0;
select cdate, cdatetime, TIMEDIFF(CDATETIME,'2007-02-28 22:23:0') from datatypetestm where TIMEDIFF(CDATETIME,'2007-02-28 22:23:0') < 0;

