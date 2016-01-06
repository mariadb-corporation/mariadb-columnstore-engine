select n_nationkey,count(s_suppkey),max(l_shipdate) 
from nation, supplier, lineitem 
where n_nationkey<=25 and s_suppkey<=50 and l_shipdate between '1994-06-14' and
'1995-05-16' and n_nationkey=s_nationkey and s_suppkey=l_suppkey
group by 1
order by 1;

