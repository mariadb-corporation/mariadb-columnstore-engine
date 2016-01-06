select distinct p_mfgr from part order by 1;
select distinct p_brand from part order by 1;
select distinct p_type from part order by 1;
select distinct p_size from part order by 1;
select distinct p_container from part order by 1;
select distinct p_type, p_brand, p_size from part order by 1,2,3;
select distinct * from part where p_partkey < 100 order by 1;
select distinct p_name from part where p_partkey < 100 order by p_name;

