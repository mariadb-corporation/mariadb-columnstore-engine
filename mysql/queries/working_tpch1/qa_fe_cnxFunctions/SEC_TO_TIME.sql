select cidx, SEC_TO_TIME(cidx) from datatypetestm;
select cidx from datatypetestm where SEC_TO_TIME(cidx) = cidx;
select cidx, CBIGINT from datatypetestm where SEC_TO_TIME(cidx) = CBIGINT;
select cidx from datatypetestm where SEC_TO_TIME(cidx) = '00:00:02';
select cidx, CVCHAR8 from datatypetestm where SEC_TO_TIME(cidx) <> CVCHAR8;