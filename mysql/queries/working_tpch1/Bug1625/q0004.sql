select count(n_nationkey), avg(c_custkey), max(c_nationkey), min(o_custkey) from  nation, customer, orders where n_nationkey = c_nationkey and c_custkey = o_custkey;

