-- 6.1.1 Standard FROM Clause Query 1
select d_date, count(*)
from lineorder,
    ( select d_date, d_datekey from dateinfo ) d
where lo_orderdate >= 19980801
      and d_datekey = lo_orderdate
group by 1 order by 1;

