-- 9.1.1 Limit Clause Query variation 1

select lo_orderkey, count(*) from lineorder
where lo_orderkey in ( select * from
       (select lo_orderkey
       from lineorder join dateinfo on lo_orderdate = d_datekey
       group by lo_orderkey
       order by sum(lo_ordtotalprice) desc
               limit 20) alias1 )
group by 1 order by 2, 1 desc;


