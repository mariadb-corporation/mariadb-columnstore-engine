--disable_query_log
if (!$func_exists_calgettrace)
{
  DROP FUNCTION calgettrace;
}
--enable_query_log

--disable_query_log
if (!$func_exists_calsettrace)
{
  DROP FUNCTION calsettrace;
}
--enable_query_log
