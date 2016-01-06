-- 8.1.2 Non-Distributed FROM Clause Query variation 2

select d_yearmonth,
       curr_count,
       prev_count,
       curr_count - prev_count days_difference
from (
       select d_yearmonth, d_monthnuminyear d_mon, count(*) curr_count
       from dateinfo where d_yearmonthnum between 199202 and 199210
       group by 1,2
       ) alias1,
       (
       select  d_monthnuminyear  +1 prev_month, count(*) prev_count
       from dateinfo where d_yearmonthnum between 199201 and 199209
       group by 1
       union
       select  d_monthnuminyear  +1 prev_month, count(*) prev_count
       from dateinfo where d_yearmonthnum between 199201 and 199209
       group by 1
       ) alias2
where alias1.d_mon = alias2.prev_month
order by d_mon;

