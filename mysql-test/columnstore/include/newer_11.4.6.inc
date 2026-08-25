# Skip test if MariaDB version is older than 11.4.6
# Query Accelerator (RBO parallel_ces) requires MariaDB >= 11.4.6
# because extractColumnStatistics() is compiled as empty stub for older versions
if (`SELECT VERSION_GET_MAJOR(@@version) < 11 OR (VERSION_GET_MAJOR(@@version) = 11 AND VERSION_GET_MINOR(@@version) < 4) OR (VERSION_GET_MAJOR(@@version) = 11 AND VERSION_GET_MINOR(@@version) = 4 AND VERSION_GET_PATCH(@@version) < 6)`)
{
  --skip Compatible only with server >= 11.4.6
}

--let $is_enterprise= `SELECT LOCATE('Enterprise', @@version_comment) OR LOCATE('enterprise', @@version)`

if (!$is_enterprise)
{
  --skip Now test requires MariaDB Enterprise Server (MCOL-6552)
}
