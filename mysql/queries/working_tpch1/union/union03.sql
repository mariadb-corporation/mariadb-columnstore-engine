select
   "nation",
   N_regionkey
from
   nation
union 
select
   "region",
   R_regionkey
from
   region order by 1, 2;

(
select
   1 as sort_col,
   N_regionkey
from
   nation
)
union all
(
select
   2,
   R_regionkey
from
   region
)
Order by sort_col, n_regionkey;

(
select
   1 as sort_col,
   N_regionkey
from
   nation
)
union all
(
select
   abs(2),
   R_regionkey
from
   region
)
Order by sort_col, n_regionkey;
