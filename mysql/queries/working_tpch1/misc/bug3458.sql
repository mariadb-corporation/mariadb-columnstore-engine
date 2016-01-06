select cidx, CAST(132.499*cidx AS DECIMAL(9,2)) from datatypetestm order by cidx;
select cidx, convert(132.499*cidx, decimal(9,2)) from datatypetestm order by cidx;

