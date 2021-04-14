# ldi ssb tables - 1g
#
LOAD DATA INFILE '/data/qa/source/ssb/1g/customer.tbl' INTO TABLE customer FIELDS TERMINATED BY '|';
LOAD DATA INFILE '/data/qa/source/ssb/1g/dateinfo.tbl' INTO TABLE dateinfo FIELDS TERMINATED BY '|';
LOAD DATA INFILE '/data/qa/source/ssb/1g/lineorder.tbl' INTO TABLE lineorder FIELDS TERMINATED BY '|';
LOAD DATA INFILE '/data/qa/source/ssb/1g/part.tbl' INTO TABLE part FIELDS TERMINATED BY '|';
LOAD DATA INFILE '/data/qa/source/ssb/1g/supplier.tbl' INTO TABLE supplier FIELDS TERMINATED BY '|';
#
