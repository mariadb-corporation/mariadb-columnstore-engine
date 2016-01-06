select count(*) from part, lineitem where (l_orderkey < 100 or l_suppkey <= l_orderkey) and p_partkey < 100 and p_size < 5 and p_partkey = l_partkey;
