-- 1
select
	l_orderkey,
	l_quantity,
	l_extendedprice
from
	nation, 
	customer,
	orders,
	lineitem
where
	c_custkey < 200
	and n_nationkey = c_nationkey 
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey;
 
-- 2) 
 select 
 	c_custkey, 
 	c_acctbal, 
 	l_orderkey, 
 	l_extendedprice 
 from 
 	nation,  
 	customer, 
 	orders, 
 	lineitem 
 where 
 	c_custkey < 1222  
 	and c_acctbal < 800  
 	and n_nationkey = c_nationkey  
 	and c_custkey = o_custkey 
 	and o_orderkey = l_orderkey;

-- 3) 
select 
 	c_custkey, 
 	c_acctbal, 
 	l_orderkey, 
 	l_extendedprice 
 from 
 	nation,  
 	customer, 
 	orders, 
 	lineitem 
 where 
 	c_custkey < 122  
 	and o_orderkey < 600000   
 	and n_nationkey = c_nationkey  
 	and c_custkey = o_custkey 
 	and o_orderkey = l_orderkey;

-- 4)  	
select 
 	c_custkey, 
 	c_acctbal, 
 	l_orderkey, 
 	l_extendedprice 
 from 
 	nation,  
 	customer, 
 	orders, 
 	lineitem 
 where 
 	c_custkey < 2  
 	and o_orderdate between '1996-07-01' and '1999-02-01' 
 	and n_nationkey = c_nationkey  
 	and c_custkey = o_custkey 
 	and o_orderkey = l_orderkey;

-- 5)
select 
 	c_custkey, 
 	c_acctbal, 
 	l_orderkey, 
 	l_extendedprice 
 from 
 	nation,  
 	customer, 
 	orders, 
 	lineitem 
 where 
 	c_custkey > 122032
 	and l_returnflag = 'R'  
 	and n_nationkey = c_nationkey  
 	and c_custkey = o_custkey 
 	and o_orderkey = l_orderkey;
 	
-- 6) 
select 
 	c_custkey, 
 	c_acctbal, 
 	l_orderkey, 
 	l_extendedprice 
 from 
 	nation,  
 	customer, 
 	orders, 
 	lineitem 
 where 
 	c_custkey < 211  
 	and o_orderkey < 600000   
 	and l_returnflag = 'R'  
 	and n_nationkey = c_nationkey  
 	and c_custkey = o_custkey 
 	and o_orderkey = l_orderkey;
 	
-- 7) 
select 
 	c_name,
 	l_extendedprice 
 from 
 	nation,  
 	customer, 
 	orders, 
 	lineitem 
 where 
 	c_custkey < 2233  
 	and o_orderkey < 600000   
 	and n_nationkey > 3 
 	and n_nationkey = c_nationkey  
 	and c_custkey = o_custkey 
 	and o_orderkey = l_orderkey;
 	
-- 8) 
select   
	c_custkey,   
	l_extendedprice   
from   
	nation,    
	customer,   
	orders,   
	lineitem   
where 
	o_orderdate > '1998-07-01' 
	and c_acctbal between 520 and 725 
	and l_returnflag = 'N'  
	and n_nationkey = c_nationkey    
	and c_custkey = o_custkey   
	and o_orderkey = l_orderkey;

-- 9) 
select    
	c_custkey,    
	l_quantity    
from    
	nation,     
	customer,    
	orders,    
	lineitem    
where  
	o_orderdate > '1996-07-01'  
	and c_acctbal between 520 and 555  
	and (l_quantity > 30 or l_returnflag = 'N' )
	and n_nationkey = c_nationkey     
	and c_custkey = o_custkey    
	and o_orderkey = l_orderkey;

-- 10) 
select
    c_custkey,
    l_quantity,
    c_acctbal
from
    nation,
    customer,
    orders,
    lineitem
where
    (c_acctbal > 9999.90 or c_acctbal < -999.99)
    and (l_quantity > 49 or l_returnflag = 'A' )
    -- and c_acctbal > 900 -- conflict with the or operators on the same table
    and n_nationkey = c_nationkey
    and c_custkey = o_custkey
    and o_orderkey = l_orderkey;
