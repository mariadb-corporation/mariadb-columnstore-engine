select lo_orderkey, count(*) 
from lineorder 
where lo_orderkey in 
( select * from (select lo_orderkey from lineorder join dateinfo on lo_orderdate = d_datekey
                group by lo_orderkey order by sum(lo_ordtotalprice) desc limit 20) alias1 )
         group by 1 order by 2, 1 desc;                           

select lo_orderkey, count(*) 
from lineorder where lo_orderkey in 
( select * from (select lo_orderkey 
from lineorder join dateinfo on lo_orderdate = d_datekey
                group by lo_orderkey order by sum(lo_ordtotalprice), lo_orderkey desc limit 2) alias1 )         
group by 1 order by 2 asc limit 1;                        

select * from 
	(select lo_orderkey, count(*) 
	from lineorder where lo_orderkey in 
		( select * from 
			(
			select lo_orderkey 
			from lineorder 
			join dateinfo 
			on lo_orderdate = d_datekey 
			where lo_orderkey <= 900
			group by lo_orderkey 
			order by sum(lo_ordtotalprice), lo_orderkey desc
			limit 5
			) alias1 
		) 
	group by lo_orderkey 
	order by 1, 2 asc 
	limit 4 
	) alias2 
order by 2, 1 desc;
