 select count(*) from dateinfo,customer, part, supplier, lineorder
     where s_suppkey = lo_suppkey
     and d_datekey = lo_orderdate
     and p_partkey = lo_partkey
     and c_custkey = lo_custkey
     and s_nation = 'BRAZIL';
