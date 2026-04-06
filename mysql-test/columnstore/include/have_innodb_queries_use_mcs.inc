# Skip test if innodb_queries_use_mcs is not ON.
# This READONLY startup variable routes InnoDB queries to Columnstore's
# select handler.  With --extern the .opt file is ignored, so the CI must
# write it into a cnf file and restart the server before running the
# future suite.

if (`SELECT NOT @@global.columnstore_innodb_queries_use_mcs`)
{
  --skip columnstore_innodb_queries_use_mcs is OFF (requires server restart with cnf)
}
