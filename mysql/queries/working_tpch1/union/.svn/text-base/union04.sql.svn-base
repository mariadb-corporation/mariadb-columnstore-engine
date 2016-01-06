select cidx, cdecimal1, cdecimal4_2, cbigint from datatypetestm
union select cidx, cdecimal9_2, cdouble, cdecimal18_2 from datatypetestm order by 1,2,3,4;

select cidx, cdate from datatypetestm union select cidx, cdatetime from datatypetestm order by 1,2;

select cidx, cdatetime from datatypetestm 
union select cidx, cdate from datatypetestm 
union select cidx, cinteger from datatypetestm order by 1,2;

select cidx, cdate, cdatetime from datatypetestm
union select cidx, cchar1, cchar2 from datatypetestm
union select cidx, cvchar255, cchar9 from datatypetestm order by 1,2,3;


-- bug3762
(select cidx, CFLOAT from datatypetestm where cidx < 3)
union (select cidx, CFLOAT from datatypetestm where cidx < 8) order by 1, 2;

(select cidx, CFLOAT from datatypetestm where cidx < 3)
union all (select cidx, CFLOAT from datatypetestm where cidx < 8) order by 1, 2;


