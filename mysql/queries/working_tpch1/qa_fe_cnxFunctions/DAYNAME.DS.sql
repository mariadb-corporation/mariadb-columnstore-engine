select cidx, CDATE, DAYNAME(CDATE) from datatypetestm;
select cidx, CDATETIME, DAYNAME(CDATETIME) from datatypetestm;
select cidx, DAYNAME('2009-02-28') from datatypetestm;
select cidx, DAYNAME('2009-07-04') from datatypetestm;
select cidx, CDATE from datatypetestm where DAYNAME(CDATE) <> cidx;
select cidx, CDATE from datatypetestm where DAYNAME(CDATE) <> cidx;
select cidx, CDATETIME from datatypetestm where DAYNAME(CDATETIME) <> CFLOAT;
select cidx, CDATE from datatypetestm where DAYNAME(CDATE) <> CCHAR8
