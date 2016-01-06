select c_custkey, o_orderkey from customer left outer join orders on c_custkey = o_custkey where c_custkey < 1000 and c_nationkey = 4 order by 1, 2;
