let $no_queryacc = `SELECT COUNT(*) = 0 FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = 'queryacc' AND ROUTINE_NAME = 'enable_queryacc'`;

if ($no_queryacc == 1)
{
  --skip queryacc routines not installed (requires install_mcs_mysql.sh)
}
