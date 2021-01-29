# This file is needed to force tests use combinations.myisam-columnstore.combinations
# Also, let's set default_storage_engine:
# - either according to the current combination
# - or to ColumnStore by default, if mtr is running
#   without combinations (e.g with --extern)


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

SET @@default_storage_engine=COALESCE(@combination,'ColumnStore');
--enable_query_log
