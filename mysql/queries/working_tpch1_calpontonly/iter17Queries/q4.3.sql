Select /*! INFINIDB_ORDERED */ min(o_orderdate), max(o_custkey) from lineitem, orders where l_partkey < 100000 and l_orderkey = o_orderkey; 
