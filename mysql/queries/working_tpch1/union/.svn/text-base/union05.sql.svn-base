select
   count(n_name),
   n_regionkey
from
   nation
group by 
   n_regionkey
union
select
   avg(r_regionkey),
   r_regionkey
from
   region
group by
   r_regionkey
order by 1,2;
   

select
   min(n_name / 1),
   n_regionkey
from
   nation
group by 
   n_regionkey
union
select
   avg(r_regionkey),
   r_regionkey
from
   region
group by
   r_regionkey
order by 1, 2;
