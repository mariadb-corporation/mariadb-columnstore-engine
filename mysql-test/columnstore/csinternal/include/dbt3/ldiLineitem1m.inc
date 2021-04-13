# ldi lineitem table
#
LOAD DATA INFILE '/data/qa/source/dbt3/1m/lineitem.tbl' INTO TABLE lineitem FIELDS TERMINATED BY '|';
