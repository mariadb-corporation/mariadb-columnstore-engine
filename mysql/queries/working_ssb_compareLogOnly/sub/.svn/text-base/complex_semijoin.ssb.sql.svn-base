select p_category, sum(lo_ordtotalprice), count(*) 
from lineorder l_outer, part 
where p_partkey = lo_partkey  and 
exists (select * from customer, lineorder l_inner
       where c_name < 'Customer#000000074' and c_custkey = l_inner.lo_custkey and 
	l_outer.lo_orderkey = l_inner.lo_orderkey) group by p_category order by 1;

select lo_linenumber, count(*) 
from lineorder l_outer 
where exists 
( select * from lineorder  l_subquery        
where l_subquery.lo_partkey = l_outer.lo_partkey and l_subquery.lo_custkey = l_outer.lo_custkey and l_subquery.lo_orderkey < 10)
and lo_orderkey <= 100 group by lo_linenumber order by 1; 

select lo_linenumber, max(lo_orderkey), min(lo_orderkey) 
from lineorder l_outer where not exists 
( select * from lineorder  l_subquery where l_subquery.lo_partkey = l_outer.lo_partkey and 
l_subquery.lo_orderkey = l_outer.lo_orderkey and l_subquery.lo_custkey <> l_outer.lo_custkey and l_subquery.lo_orderkey < 10) and
lo_orderkey between 10000 and 20100 group by lo_linenumber order by 1;

select lo_linenumber, count(*) 
from lineorder l_outer 
where l_outer.lo_orderkey <= 91000 and 
exists ( select * from lineorder  l_subquery where l_subquery.lo_shippriority = l_outer.lo_shippriority and 
	l_subquery.lo_linenumber = l_outer.lo_linenumber and 
	l_subquery.lo_custkey = l_outer.lo_custkey and 
	l_subquery.lo_partkey <> l_outer.lo_partkey and l_subquery.lo_orderkey < 99110) group by lo_linenumber order by 1;

select min(lo_orderkey), max(lo_orderkey), sum(lo_linenumber) 
from lineorder l_outer where lo_orderkey between 1010 and 91050 and 
lo_suppkey in 
( select lo_suppkey from lineorder  l_subquery 
  where l_subquery.lo_suppkey = l_outer.lo_suppkey and l_subquery.lo_linenumber = l_outer.lo_linenumber and 
        l_subquery.lo_commitdate > l_outer.lo_commitdate and l_subquery.lo_orderkey < 99910);

select lo_orderpriority, count(*) from lineorder l_outer 
where round(lo_orderkey, -3) = lo_orderkey and lo_orderkey <= 400000 and 
lo_orderkey not in 
( select lo_orderkey 
from lineorder  l_subquery 
where l_subquery.lo_partkey = l_outer.lo_partkey and 
l_subquery.lo_custkey <> l_outer.lo_custkey and l_subquery.lo_orderkey > 99000) 
group by lo_orderpriority order by 1;

select lo_orderpriority, count(*) from lineorder lo_outer 
where lo_outer.lo_orderkey <= 2000 and
      (lo_orderkey, lo_linenumber) in 
( select lo_orderkey, max(lo_linenumber)
  from lineorder lo_subquery where lo_orderkey < 10000 and 
       lo_outer.lo_discount = lo_subquery.lo_discount group by lo_orderkey ) 
group by lo_orderpriority
order by 1;
