select count(s_suppkey), min(s_suppkey), max(s_suppkey), sum(s_suppkey), avg(s_suppkey) from supplier;
select count(s_name), min(s_name), max(s_name) from supplier;
select count(s_address), min(s_address), max(s_address) from supplier;
select count(s_city), min(s_city), max(s_city) from supplier;
select count(s_nation), min(s_nation), max(s_nation) from supplier;
select count(s_region), min(s_region), max(s_region) from supplier;
select count(s_phone), min(s_phone), max(s_phone) from supplier;

