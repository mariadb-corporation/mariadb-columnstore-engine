-- basic
select hex(r_name), hex(r_regionkey), hex(r_comment) from region;
-- null handling
select hex(c2) from sub1;
select hex(r_name) from region where hex(r_name) > '414652494341';
select hex(r_name) from region where hex(r_name) > '0x414652494341';
select hex(r_name) from region where hex(r_name) > 414652494341;
select hex(cchar5) from datatypetestm order by hex(cchar8);
select hex(r_name), count(*) from region group by 1 order by 1;
select cidx, CDATE, HEX(CDATE) from datatypetestm order by cidx;
select cidx, CDATETIME, HEX(CDATETIME) from datatypetestm order by cidx;
select cidx, CDECIMAL1, HEX(CDECIMAL1) from datatypetestm order by cidx;
select cidx, CDECIMAL4_2, HEX(CDECIMAL4_2) from datatypetestm order by cidx;
select cidx, CDECIMAL9_2, HEX(CDECIMAL9_2) from datatypetestm order by cidx;
