# ldi dbt3 tables - 1m
#
LOAD DATA INFILE '/data/qa/source/dbt3/1m/nation.tbl' INTO TABLE nation FIELDS TERMINATED BY '|';
LOAD DATA INFILE '/data/qa/source/dbt3/1m/region.tbl' INTO TABLE region FIELDS TERMINATED BY '|';
LOAD DATA INFILE '/data/qa/source/dbt3/1m/customer.tbl' INTO TABLE customer FIELDS TERMINATED BY '|';
LOAD DATA INFILE '/data/qa/source/dbt3/1m/orders.tbl' INTO TABLE orders FIELDS TERMINATED BY '|';
LOAD DATA INFILE '/data/qa/source/dbt3/1m/supplier.tbl' INTO TABLE supplier FIELDS TERMINATED BY '|';
LOAD DATA INFILE '/data/qa/source/dbt3/1m/partsupp.tbl' INTO TABLE partsupp FIELDS TERMINATED BY '|';
LOAD DATA INFILE '/data/qa/source/dbt3/1m/part.tbl' INTO TABLE part FIELDS TERMINATED BY '|';
LOAD DATA INFILE '/data/qa/source/dbt3/1m/lineitem.tbl' INTO TABLE lineitem FIELDS TERMINATED BY '|';
#
