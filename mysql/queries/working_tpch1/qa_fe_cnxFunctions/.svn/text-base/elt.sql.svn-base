select cidx, CCHAR5, ELT(4,cbigint,1,2,10) from datatypetestm;
select cidx, CCHAR1, ELT(1,CCHAR1) from datatypetestm;
select cidx, CCHAR2, ELT(10,CCHAR2,CCHAR4) from datatypetestm;
select cidx, CCHAR3, ELT(0,CCHAR3) from datatypetestm;
select cidx, CCHAR4, ELT(-1,CCHAR4,CCHAR4) from datatypetestm;
select cidx, CCHAR5, ELT(4,CCHAR5,CVCHAR255,'s',CVCHAR5) from datatypetestm;
select cidx, CCHAR1 from datatypetestm where ELT(4,CCHAR5,CVCHAR255,'s',CVCHAR5) <> CCHAR1;
select cidx, CCHAR1 from datatypetestm where ELT(45,CCHAR5,CVCHAR255,'s',CVCHAR5) <> CCHAR1;

