select (select d_date from dateinfo 
  where d_datekey = lo_orderdate) d_date,
     count(*) 
from lineorder where lo_orderdate >= 19980101 
group by 1
order by 1
;
