# Runs the sql files in the working_tpch1 directory.
# Dependencies:
# 1) A database named tpch1 containing the tpch tables and the datatypetestm table.
#
# Arguments:
#   schema to run against (optional)

DB=tpch1
if [ -n "$1" ] ; then
        DB=$1
fi

./queryTester -q working_tpch1 -u $DB -r Tr -g
