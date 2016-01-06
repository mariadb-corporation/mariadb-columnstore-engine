select count(c_custkey), min(c_custkey), max(c_custkey), sum(c_custkey), avg(c_custkey) from customer;
select count(c_name), min(c_name), max(c_name) from customer;
select count(c_address), min(c_address), max(c_address) from customer;
select count(c_city), min(c_city), max(c_city) from customer;
select count(c_nation), min(c_nation), max(c_nation) from customer;
select count(c_region), min(c_region), max(c_region) from customer;
select count(c_phone), min(c_phone), max(c_phone) from customer;
select count(c_mktsegment), min(c_mktsegment), max(c_mktsegment) from customer;

