--disable_warnings
drop table if exists jsontest;
--enable_warnings
#
CREATE TABLE jsontest (
cInt INT, 
cVarchar VARCHAR(300), 
cText TEXT) 
ENGINE=columnstore;
#
