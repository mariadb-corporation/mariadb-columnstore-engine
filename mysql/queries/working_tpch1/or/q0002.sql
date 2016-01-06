select count(*) from part,lineitem where (l_orderkey < 100 or l_suppkey <= 1) and p_partkey = l_partkey;
