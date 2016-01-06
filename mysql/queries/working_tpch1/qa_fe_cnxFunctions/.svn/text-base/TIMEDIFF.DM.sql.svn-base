select cidx, CDATE, TIMEDIFF(CDATE,'2007-02-28 22:23:0') from datatypetestm;
select cidx, CDATETIME, TIMEDIFF(CDATETIME,'2007-02-28 22:23:0') from datatypetestm;
select cidx, CDATE from datatypetestm where TIMEDIFF(CDATE,'2007-02-28 22:23:0') <> CDATE;
select cidx, CDATETIME from datatypetestm where TIMEDIFF(CDATETIME,'2007-02-28 22:23:0') <> CDATETIME;
select *, timediff(CDATE, CDATETIME) from datatypetestm  where isnull(timediff(CDATE, CDATETIME));
select * from datatypetestm  where timediff(CDATE, CDATETIME) is null;

