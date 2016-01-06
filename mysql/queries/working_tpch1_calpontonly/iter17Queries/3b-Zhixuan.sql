select
	l_orderkey,
	l_quantity,
	l_extendedprice
from
	region,
	nation, 
	customer,
	orders,
	lineitem
where
	c_custkey < 1242
	and r_regionkey = n_regionkey
	and n_nationkey = c_nationkey 
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey;
 
 select 
 	avg(l_extendedprice), 
 	o_orderdate
 from 
  region,
 	nation,  
 	customer, 
 	orders, 
 	lineitem 
 where 
 	c_custkey < 1112  
 	and c_acctbal < 800  
 	and r_regionkey = n_regionkey
 	and n_nationkey = c_nationkey  
 	and c_custkey = o_custkey 
 	and o_orderkey = l_orderkey
 group by o_orderdate;
 
select 
 	n_name,
 	r_name, 
 	l_extendedprice 
 from 
 	region,
 	nation,  
 	customer, 
 	orders, 
 	lineitem 
 where 
 	c_acctbal < -8  
 	and l_extendedprice < 960   
 	and r_regionkey = n_regionkey
 	and n_nationkey = c_nationkey  
 	and c_custkey = o_custkey 
 	and o_orderkey = l_orderkey;

select 
 	c_custkey, 
 	o_orderkey
 from 
 	region,
 	nation,  
 	customer, 
 	orders, 
 	lineitem 
 where 
 	n_name = 'CHINA'
 	and c_mktsegment = 'AUTOMOBILE'
 	and c_custkey < 8821
 	and r_regionkey = n_regionkey
 	and n_nationkey = c_nationkey  
 	and c_custkey = o_custkey 
 	and o_orderkey = l_orderkey;

select 
 	count(l_returnflag)
 	name 
 from 
  region,
 	nation,  
 	customer, 
 	orders, 
 	lineitem 
 where 
 	c_custkey < 11112  
 	and l_returnflag = 'R'
 	and r_regionkey = n_regionkey  
 	and n_nationkey = c_nationkey  
 	and c_custkey = o_custkey 
 	and o_orderkey = l_orderkey
 group by
 	l_returnflag,
 	n_name;
 
