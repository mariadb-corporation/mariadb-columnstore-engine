# ldi nation table
#
LOAD DATA INFILE '/data/qa/source/dbt3/1m/nation.tbl' INTO TABLE nation FIELDS TERMINATED BY '|';
