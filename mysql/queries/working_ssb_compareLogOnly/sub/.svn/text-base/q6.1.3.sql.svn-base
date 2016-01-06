-- 6.1.3 Standard FROM Clause Query variation 3

select c_region, count(*)
from lineorder, customer, 
    ( select d1.d_date, d2.d_datekey
    from dateinfo d1, dateinfo d2
    where d1.d_datekey = d2.d_datekey ) d
where lo_orderdate >= 19980801
      and d_datekey = lo_orderdate
      and lo_custkey = c_custkey
group by 1 order by 1;

