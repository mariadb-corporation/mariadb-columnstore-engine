select cdate, makedate(year(cdate),1) from datatypetestm;
select cdatetime, makedate(year(cdatetime),1) from datatypetestm;
select cdate, makedate(year(cdate),0) from datatypetestm;
select cdate, makedate(year(cdate),-1) from datatypetestm;
select cfloat, makedate(year(cfloat),1) from datatypetestm;
select cdate, makedate(year(cdate),1) from datatypetestm where makedate(year(cdate),1) <> 0;
select cdatetime, makedate(year(cdatetime),1) from datatypetestm where makedate(1900,1) <> 0;
select cdatetime, makedate(year(cdatetime),1) from datatypetestm where makedate(2011,1) <> 0;
select cdatetime, makedate(year(cdatetime),1) from datatypetestm where makedate(2011,1) = '1970-01-01';

select makedate(2011, CDECIMAL1) from datatypetestm;
select makedate(2011, CDECIMAL4) from datatypetestm;
select makedate(2011, CDECIMAL4_2) from datatypetestm;
select makedate(2011, CDECIMAL5) from datatypetestm;
select makedate(2011, CDECIMAL9) from datatypetestm;
select makedate(2011, CDECIMAL9_2) from datatypetestm;
select makedate(2011, CDECIMAL10) from datatypetestm;
select makedate(2011, CDECIMAL18) from datatypetestm;
select makedate(2011, CDECIMAL18_2) from datatypetestm;
select cidx, CDATE, MAKEDATE(2010, CDATE) from datatypetestm order by cidx;
select cidx, CCHAR1 from datatypetestm where MAKEDATE(2010, CCHAR1) <> CCHAR1 order by cidx;

select cdate, date(makedate(year(cdate),1)) from datatypetestm;
select cdate, date(makedate(year(cdate),0)) from datatypetestm;
select cdate, date(makedate(year(cdate),-1)) from datatypetestm;

