select cast(cdatetime as signed) from datatypetestm;
select (cdatetime << 8) from datatypetestm;
select (cdatetime & 0xff0000000000) from datatypetestm;
select (cdatetime >> 12) from datatypetestm;
select (cdatetime | 0xffffff) from datatypetestm;
select (cdatetime ^ 337) from datatypetestm;
select bit_count(cdatetime) from datatypetestm;
