-- 2.7.1 Scalar Having Clause query 1

select p_category, sum(lo_ordtotalprice), count(*)
from lineorder, part
where p_partkey = lo_partkey
group by 1
having sum(lo_ordtotalprice) >
        (select avg(lo_ordtotalprice) * 1.2  from lineorder
        where lo_orderdate = 19940101 )
order by 1;

