select l_linenumber from lineitem l left join(select n_nationkey,
n_regionkey from nation n left join customer c on n.n_nationkey=c.c_nationkey  
  where c.c_custkey < 1000) as t1 on l_linenumber = t1.n_regionkey left join
(select o_orderkey from    orders o where o_orderkey < 5000 and o_custkey
between 3 and 600) as t2 on l.l_orderkey=t2.o_orderkey where l.l_partkey< 100;

select l_linenumber from lineitem l left join(select n_nationkey,
n_regionkey from nation n left join customer c on n.n_nationkey=c.c_nationkey  
  where c.c_custkey < 1000) as t1 on l_linenumber = t1.n_regionkey left join
(select o_orderkey from    orders o where o_orderkey < 5000 and o_custkey
between 3 and 600) as t2 on l.l_orderkey=t2.o_orderkey where l.l_partkey< 100;


