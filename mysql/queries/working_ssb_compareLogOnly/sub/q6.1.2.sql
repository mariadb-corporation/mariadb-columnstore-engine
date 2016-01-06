-- 6.1.2 Standard FROM Clause Query variation 2

select d_date, count(*)
from lineorder,
    ( select d1.d_date, d2.d_datekey
    from dateinfo d1, dateinfo d2
    where d1.d_datekey = d2.d_datekey ) d
where lo_orderdate >= 19980801
      and d_datekey = lo_orderdate
group by 1 order by 1;

