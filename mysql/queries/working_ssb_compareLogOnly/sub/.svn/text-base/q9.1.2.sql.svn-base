/*
9.1.2 Limit Clause Query variation 2
Show the 2nd ranked item only. Note the ultimate limit can still be post-processed.
*/

select lo_orderkey, count(*) from lineorder
where lo_orderkey in ( select * from
       (select lo_orderkey
       from lineorder join dateinfo on lo_orderdate = d_datekey
       group by lo_orderkey
-- Added lo_orderkey to subselect order by clause for consistent results.
--       order by sum(lo_ordtotalprice) desc
       order by sum(lo_ordtotalprice) desc, lo_orderkey
               limit 2) alias1 )
group by 1 order by 2 asc, 1 limit 1;
