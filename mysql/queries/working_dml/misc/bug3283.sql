# Negative test case for attempting to drop a non-existent partition.
select caldroppartitions('lineitem', '4.1.1');
select caldisablepartitions('lineitem', '4.1.1');

