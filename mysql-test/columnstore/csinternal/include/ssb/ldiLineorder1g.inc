# ldi lineorder table
#
LOAD DATA INFILE '/data/qa/source/ssb/1g/lineorder.tbl' INTO TABLE lineorder FIELDS TERMINATED BY '|';
