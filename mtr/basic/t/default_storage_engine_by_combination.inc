--disable_query_log
SET @combination=NULL;
if ($MTR_COMBINATION_MYISAM)
{
  SET @combination="MYISAM";
}
if ($MTR_COMBINATION_COLUMNSTORE)
{
  SET @combination="COLUMNSTORE";
}
#
# If we're running without combinations, e.g. with --extern,
# then set default_storage_engine to ColumnStore.
# Otherwise (when running without --extern) it's set by the *.combinations file.
# 
SET @@default_storage_engine=COALESCE(@combination,'ColumnStore');
--enable_query_log
