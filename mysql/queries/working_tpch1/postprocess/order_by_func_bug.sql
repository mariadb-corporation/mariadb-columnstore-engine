select cidx, CCHAR1 from datatypetestm where cidx>5 order by POSITION('aaa' IN CCHAR1), cidx;
select cidx, WEEKOFYEAR(CDATE), CDATE from datatypetestm where cidx>5 order by WEEKOFYEAR(CDATE), cidx;
select cidx, CDATE from datatypetestm where cidx>5 order by SUBTIME(CDATE,'1:14:25'), cidx;
select cidx, CBIGINT from datatypetestm where cidx>5 order by TRUNCATE(CBIGINT,-2), cidx;


