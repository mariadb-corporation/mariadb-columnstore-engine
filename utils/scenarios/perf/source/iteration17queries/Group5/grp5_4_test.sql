select count(*) from part, lineitem
	where p_retailprice < 913.65 
	and  p_partkey = l_suppkey 
	and l_shipdate < '1993-04-07';
Select calgetstats();
Select now();
select count(*) from part, lineitem
	where p_retailprice < 913.65 
	and  p_partkey = l_suppkey 
	and l_shipdate < '1993-04-07';
Select calgetstats();
quit
