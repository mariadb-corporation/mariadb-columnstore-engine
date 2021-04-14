# set the count of collations in a variable for a character set
let $col_count = `SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLLATIONS WHERE CHARACTER_SET_NAME LIKE '$char_set'`;

#create table with all supported collations for a character set
if(!$column_flag)
{
while($col_count)
{
 let $i = `SELECT $col_count-1`;
 let $collation = `SELECT COLLATION_NAME FROM INFORMATION_SCHEMA.COLLATIONS WHERE CHARACTER_SET_NAME LIKE '$char_set' LIMIT $i,1`;
 eval CREATE TABLE t_$col_count$char_set(name VARCHAR(10)) CHARSET $char_set COLLATE '$collation' ENGINE=Columnstore;
 eval SHOW CREATE TABLE t_$col_count$char_set;
 dec $col_count;
}
}

#create table with all supported collations for a character set at column level
if($column_flag)
{
while($col_count)
{
 let $i = `SELECT $col_count-1`;
 let $collation = `SELECT COLLATION_NAME FROM INFORMATION_SCHEMA.COLLATIONS WHERE CHARACTER_SET_NAME LIKE '$char_set' LIMIT $i,1`;
 eval CREATE TABLE tcol_$col_count$char_set (name CHAR(3) CHARACTER SET $char_set COLLATE '$collation') ENGINE=Columnstore;
 eval SHOW CREATE TABLE tcol_$col_count$char_set;
 dec $col_count;
}
}
