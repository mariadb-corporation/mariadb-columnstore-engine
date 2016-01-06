select
   n_regionkey
from
   nation
where
   n_nationkey <= 5
union 
select
   n_regionkey
from
   nation
where
   n_nationkey >= 20
order by 1;
   
select
   n_regionkey
from
   nation
where
   n_nationkey <= 5
union all
select
   n_nationkey
from
   nation
where
   n_nationkey >= 20
order by 1;

select
   n_regionkey,
	 n_nationkey
from
   nation
where
   n_nationkey <= 5
union 
select
   n_regionkey,
   n_nationkey
from
   nation
where
   n_nationkey >= 20
order by 1,2;

select
   n_regionkey,
	 n_nationkey
from
   nation
where
   n_nationkey <= 5
union all
select
   n_regionkey,
   n_nationkey
from
   nation
where
   n_nationkey >= 20
order by 1,2;
