select count(p_partkey), min(p_partkey), max(p_partkey), sum(p_partkey), avg(p_partkey) from selectIntoImport;
select count(p_name), min(p_name), max(p_name) from selectIntoImport;
select count(p_mfgr), min(p_mfgr), max(p_mfgr) from selectIntoImport;
select count(p_category), min(p_category), max(p_category) from selectIntoImport;
select count(p_brand1), min(p_brand1), max(p_brand1) from selectIntoImport;
select count(p_color), min(p_color), max(p_color) from selectIntoImport;
select count(p_type), min(p_type), max(p_type) from selectIntoImport;
select count(p_size), min(p_size), max(p_size) from selectIntoImport;
select count(p_container), min(p_container), max(p_container) from selectIntoImport;

