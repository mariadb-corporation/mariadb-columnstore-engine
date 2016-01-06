select quarter(cdate), quarter(cdatetime) from datatypetestm;
select cidx, cdate, cdatetime  from datatypetestm where quarter(cdate) > 2;
select cidx, cdate, cdatetime  from datatypetestm where quarter(cdatetime) > 2;

