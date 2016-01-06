# Runs the sql files in the working_tpch1_calpontOnly directory.
#
# Notes:
# 1) These scripts are run against the tpch1 schema and do any comparisons to a reference database.
#
# Dependencies:
# 1) A database named tpch1 containing the tpch tables.
#
# Arguments:
#   schema to run against (optional)

DB=tpch1
if [ -n "$1" ] ; then
        DB=$1
fi

./queryTester -q working_tpch1_calpontonly -u $DB -r T -g
