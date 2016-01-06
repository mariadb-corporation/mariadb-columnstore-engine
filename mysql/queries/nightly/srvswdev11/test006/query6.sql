select 'query6', batch, loaddt, min(c1), max(c1), count(c1), count(c2), count(c3), count(c4), count(c5) 
from test006
where c4 > 0  and loaddt >= subtime(now(), 420)
group by 1, 2, 3
order by 2; 

