# ldi customer table
#
LOAD DATA INFILE '/data/qa/source/dbt3/1g/customer.tbl' INTO TABLE customer FIELDS TERMINATED BY '|';
