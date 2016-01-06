set autocommit=0;
update datatypetestm set cchar255=convert(cdate,date),cvchar255=convert(cdatetime,date);
select cchar255, cvchar255 from datatypetestm;
rollback;
update datatypetestm set cidx=cidx*10, CCHAR255=DATE(CDATE) where DATE(CDATE) >0;
select cidx, cchar255 from datatypetestm;
rollback;

