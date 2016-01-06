select l_shipdate Revenue_day,
       l_discount district,
        max(l_extendedprice) max_rev_per_day,
        min(l_extendedprice) min_rev_per_day,
        avg(l_extendedprice) avg_rev_per_day,
        sum(l_extendedprice) Total_Revenue,
        count(*) Sales_items
from lineitem
where l_orderkey<100
group by l_shipdate, l_discount
order by total_revenue DESC, Revenue_day;
