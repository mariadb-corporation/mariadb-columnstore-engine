-- ignore individual order by
select
   n_regionkey,
	 n_nationkey
from
   nation
where
   n_nationkey <= 5
union all
select
   r_name,
   r_regionkey
from
   region
Order By
   n_regionkey, n_nationkey;

-- Post process main query order by
(
select
   n_regionkey,
	 n_nationkey
from
   nation
where
   n_nationkey <= 5
Order By
   N_regionkey
)
union
(
select
   r_name,
   r_regionkey
from
   region
Order By
   R_name
)
Order by n_regionkey, n_nationkey limit 3;
