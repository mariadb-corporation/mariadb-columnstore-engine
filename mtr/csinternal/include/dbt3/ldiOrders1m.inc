# ldi orders table
#
LOAD DATA INFILE '/data/qa/source/dbt3/1m/orders.tbl' INTO TABLE orders FIELDS TERMINATED BY '|';
