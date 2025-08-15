let $singlenode = `SELECT mcs_nodes_count() = 1`;

if (!$singlenode)
{
  --skip Test requires single node execution.
}
