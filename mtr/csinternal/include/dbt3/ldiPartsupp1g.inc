# ldi partsupp table
#
LOAD DATA INFILE '/data/qa/source/dbt3/1g/partsupp.tbl' INTO TABLE partsupp FIELDS TERMINATED BY '|';
