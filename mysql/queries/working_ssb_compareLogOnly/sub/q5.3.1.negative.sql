-- 5.3.1 Correlated Scalar Negative Case . Missing Join
select count(*)
from lineorder t1
where lo_orderkey <
        ( select lo_orderkey from lineorder t2
                Where t1.lo_partkey <> t2.lo_partkey
                and   t1.lo_custkey  > t2.lo_custkey )

