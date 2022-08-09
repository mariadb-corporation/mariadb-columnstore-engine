# ldi supplier table
#
LOAD DATA INFILE '/data/qa/source/dbt3/1m/supplier.tbl' INTO TABLE supplier FIELDS TERMINATED BY '|';
