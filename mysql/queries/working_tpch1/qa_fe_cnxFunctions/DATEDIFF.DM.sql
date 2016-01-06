select cidx, CDATE, DATEDIFF(CDATE,'2007-02-28') from datatypetestm;
select cidx, CDATE, DATEDIFF(CDATE,'2007-07-04') from datatypetestm;
select cidx, CDATETIME, DATEDIFF(CDATETIME,'2007-02-28') from datatypetestm;
select cidx, CDATETIME, DATEDIFF(CDATETIME,'2007-07-04') from datatypetestm;
select cidx, CDATE from datatypetestm where DATEDIFF(CDATE,'2007-02-28') <> CDATE;
select cidx, CDATE from datatypetestm where DATEDIFF(CDATE,'2007-07-04') <> CDATE;
select cidx, CDATETIME from datatypetestm where DATEDIFF(CDATETIME,'2007-02-28') <> CDATETIME;
select cidx, CDATETIME from datatypetestm where DATEDIFF(CDATETIME,'2007-07-04') <> CDATETIME;
