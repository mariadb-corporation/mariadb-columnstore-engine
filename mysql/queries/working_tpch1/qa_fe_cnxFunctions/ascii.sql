select ascii(r_regionkey) from region where ascii(r_name)>65;
select ascii(c2) from sub1;
select ascii(cdate), ascii(cdecimal1), ascii(cdecimal9_2), ascii(cchar8), ascii(cinteger) from datatypetestm;
select * from region order by ascii(r_comment);

