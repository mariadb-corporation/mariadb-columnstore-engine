set autocommit=0;
select 'q1', nation.* from nation;
update nation set n_comment='x' where n_nationkey <= n_regionkey;
select 'q2', nation.* from nation;
rollback;
select 'q3', nation.* from nation;
update nation set n_comment='x' where n_nationkey = n_regionkey;
select 'q4', nation.* from nation;
rollback;
select 'q5', nation.* from nation;
update nation set n_comment='x' where n_nationkey > n_regionkey;
select 'q6', nation.* from nation;
rollback;
select 'q7', count(*) from orders where  o_orderkey<o_custkey and o_shippriority < o_custkey;
update orders set o_orderkey=o_custkey where  o_orderkey<o_custkey and o_shippriority < o_custkey;
select 'q8', count(*) from orders where  o_orderkey=o_custkey and o_shippriority < o_custkey;
rollback;

