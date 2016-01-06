select d_date, count(*) from lineorder, ( select d_date, d_datekey from dateinfo ) d where lo_orderdate >= 19980801 and d_datekey = lo_orderdate group by 1 order by 1;                                                                                               

select d_date, count(*) from lineorder, ( select d1.d_date, d2.d_datekey from dateinfo d1, dateinfo d2 where d1.d_datekey = d2.d_datekey ) d where lo_orderdate >= 19980801 and d_datekey = lo_orderdate group by 1 order by 1;

select c_region, count(*) from lineorder, customer, ( select d1.d_date, d2.d_datekey from dateinfo d1, dateinfo d2 where d1.d_datekey = d2.d_datekey ) d where lo_orderdate >= 19980801 and d_datekey = lo_orderdate and lo_custkey = c_custkey group by 1 order by 1;

select cnt, count(*) from (select d_month, count(*) cnt from dateinfo where d_year = 1992 group by 1 order by 1) sq group by 1 order by 1 desc;                                                                                                                       
Select * from (Select count(distinct(cnt)) from ( select cnt, count(*) from (select d_month, count(*) cnt from dateinfo where d_year = 1992 group by 1 order by 1) sq  group by 1 order by 1 desc) sq2 ) sq3 ;                                                        

Select * from (Select count(distinct(cnt)) from ( select cnt, count(*) from (select d_month, count(*) cnt from dateinfo where d_year = 1992 group by 1 order by 1) sq group by 1 order by 1 desc) sq2 ) sq3 ;                                                         
