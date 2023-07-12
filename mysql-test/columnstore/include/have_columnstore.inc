let $notcolumnstore = `SELECT COUNT(*) = 0 FROM INFORMATION_SCHEMA.ENGINES WHERE engine = 'columnstore' AND support IN ('YES', 'DEFAULT', 'ENABLED')`;

if ($notcolumnstore == 1)
{
  --skip Test requires engine ColumnStore.
}
