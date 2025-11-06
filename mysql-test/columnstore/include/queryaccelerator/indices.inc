# Create indexes for query optimization
ALTER TABLE partsupp ADD INDEX I1 (PS_PARTKEY, PS_SUPPKEY);
ALTER TABLE lineitem ADD INDEX(`l_orderkey`, `l_suppkey`);
ALTER TABLE orders   ADD INDEX(`o_orderkey`, `o_orderstatus`);
ALTER TABLE supplier ADD INDEX(s_nationkey, s_suppkey,s_name);
SET histogram_size=10;
ANALYZE TABLE lineitem PERSISTENT FOR COLUMNS(l_orderkey) INDEXES();
SET histogram_size=3;
ANALYZE TABLE partsupp PERSISTENT FOR COLUMNS(ps_partkey) INDEXES();
SET histogram_size=2;
ANALYZE TABLE part PERSISTENT FOR COLUMNS(p_partkey) INDEXES();
SET histogram_size=5;
ANALYZE TABLE orders PERSISTENT FOR COLUMNS(o_orderkey) INDEXES();
