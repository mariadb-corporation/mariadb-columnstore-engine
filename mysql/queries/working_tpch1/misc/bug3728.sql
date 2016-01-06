select 'l_shipinstruct', count(l_shipinstruct)+sum(if(l_shipinstruct is null,1, 0)) from lineitem;

