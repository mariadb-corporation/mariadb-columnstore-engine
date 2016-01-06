select cidx, cbigint, avg(cbigint) from datatypetestm group by cidx, cbigint order by 1;
select avg(cbigint) from datatypetestm where cidx=1;
