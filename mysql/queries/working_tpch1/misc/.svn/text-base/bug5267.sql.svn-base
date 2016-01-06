select n_nationkey, case when n_nationkey xor n_regionkey then 1 else 0 end case_xor from nation;
select n_nationkey, if(n_nationkey xor n_regionkey, 1, 0) from nation;
select n_nationkey, if(n_nationkey and n_regionkey, 1, 0) from nation;
select n_nationkey, case when n_nationkey or n_regionkey then 1 else 0 end case_xor from nation;

