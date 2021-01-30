let $notcolumnstore = `SELECT COUNT(*) = 0 FROM INFORMATION_SCHEMA.ENGINES WHERE engine = 'columnstore' AND support IN ('YES', 'DEFAULT', 'ENABLED')`;

if ($notcolumnstore == 1)
{
  --skip Test requires engine ColumnStore.
}

--disable_query_log
set global character_set_server='latin1';
--enable_query_log
