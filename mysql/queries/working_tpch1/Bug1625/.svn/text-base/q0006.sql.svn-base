select n_name, count(c_custkey) from  nation, customer, orders  where n_nationkey > n_regionkey and n_nationkey = c_nationkey and c_custkey = o_custkey group by n_name  order by 1;
