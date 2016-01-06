/* 
A from clause subquery does not use semi-join behavior, instead uses standard join behavior.  Id does, however specify small side hash map behavior and determine what subtrees should be materialized.  A simple representation with the materialized value highlighted in yellow.
*/

select d_date, count(*)
from lineorder,
    ( select d_date, d_datekey from dateinfo ) d
where lo_orderdate >= 19980801
      and d_datekey = lo_orderdate
group by 1 order by 1;


