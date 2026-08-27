--disable_query_log
SET character_set_server = "utf8mb4";
--enable_query_log

let $notcolumnstore = `SELECT COUNT(*) = 0 FROM INFORMATION_SCHEMA.ENGINES WHERE engine = 'columnstore' AND support IN ('YES', 'DEFAULT', 'ENABLED')`;

if ($notcolumnstore == 1)
{
  --skip Test requires engine ColumnStore.
}
