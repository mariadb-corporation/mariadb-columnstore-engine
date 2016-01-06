/*
9.1.3 Limit Clause Query variation 3
Show the 2-5 items only.  Here, because of the additional from clause nesting, the 2nd limit would be processed by InfiniDB.
*/

select * from (
         select lo_orderkey, count(*) from lineorder
         where lo_orderkey in ( select * from
                ( select lo_orderkey
                  from lineorder join dateinfo on lo_orderdate = d_datekey
                  group by lo_orderkey
--                order by sum(lo_ordtotalprice) desc
                  order by sum(lo_ordtotalprice) desc, lo_orderkey
                  limit 5 ) alias1 )
--       group by 1 order by 2 asc limit 4 ) alias2
         group by 1 order by 2 asc, 1 limit 4 ) alias2
-- order by 2 desc;
order by 2 desc, 1;
