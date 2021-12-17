#
# Functions calgettrace() and calsettrace():
# - normally exist when "mtr --extern" is running
# - do not exist when mtr without --extern is running
#
# This file creates the functions if they do not exist.
#

let $func_exists_calsettrace=`SELECT COUNT(*) FROM mysql.func WHERE name='calsettrace'`;
--disable_query_log
if (!$func_exists_calsettrace)
{
  CREATE FUNCTION calsettrace RETURNS INTEGER SONAME "ha_columnstore.so";
}
--enable_query_log


let $func_exists_calgettrace=`SELECT COUNT(*) FROM mysql.func WHERE name='calgettrace'`;
--disable_query_log
if (!$func_exists_calgettrace)
{
  CREATE FUNCTION calgettrace RETURNS STRING SONAME "ha_columnstore.so";
}
--enable_query_log

