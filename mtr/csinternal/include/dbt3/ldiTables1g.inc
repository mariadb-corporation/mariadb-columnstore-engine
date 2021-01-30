# ldi dbt3 tables - 1g
#
LOAD DATA INFILE '/data/qa/source/dbt3/1g/nation.tbl' INTO TABLE nation FIELDS TERMINATED BY '|';
LOAD DATA INFILE '/data/qa/source/dbt3/1g/region.tbl' INTO TABLE region FIELDS TERMINATED BY '|';
LOAD DATA INFILE '/data/qa/source/dbt3/1g/customer.tbl' INTO TABLE customer FIELDS TERMINATED BY '|';
LOAD DATA INFILE '/data/qa/source/dbt3/1g/orders.tbl' INTO TABLE orders FIELDS TERMINATED BY '|';
LOAD DATA INFILE '/data/qa/source/dbt3/1g/supplier.tbl' INTO TABLE supplier FIELDS TERMINATED BY '|';
LOAD DATA INFILE '/data/qa/source/dbt3/1g/partsupp.tbl' INTO TABLE partsupp FIELDS TERMINATED BY '|';
LOAD DATA INFILE '/data/qa/source/dbt3/1g/part.tbl' INTO TABLE part FIELDS TERMINATED BY '|';
LOAD DATA INFILE '/data/qa/source/dbt3/1g/lineitem.tbl' INTO TABLE lineitem FIELDS TERMINATED BY '|';
#
