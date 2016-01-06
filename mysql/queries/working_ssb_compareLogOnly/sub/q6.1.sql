/*
6.1 Standard From Clause Graphic
The graphic for the above query specifies that the d_date and d_datekey columns be materialized prior to joining to lineorder, which is what happens anyway when dateinfo is the small side. Remaining functionality is unchanged, in fact the query re-written as the following will execute the identical plan and return identical rows.
-- STD JOIN
*/

select d_date, count(*)
from lineorder, dateinfo
where lo_orderdate >= 19980801
      and d_datekey = lo_orderdate
group by 1 order by 1;

