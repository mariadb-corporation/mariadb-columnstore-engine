select md5(cinteger), md5(cbigint), md5(cdecimal4), md5(cdecimal18_2), md5(cchar4), md5(cvchar255), md5(cdate), md5(cdatetime) from datatypetestm;
select cidx, cdate, cdatetime from datatypetestm where md5(cdate) = md5(cdatetime);
select cidx from datatypetestm order by md5(cdecimal18);
