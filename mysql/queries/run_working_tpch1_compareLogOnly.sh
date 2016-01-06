# Runs the sql files in the working_tpch1_cmpareLogOnly directory.
#
# Notes:
# 1) Runs queries against the tpch1 schema.
# 2) Compares to checked in reference files.  Running these scripts against the reference database takes a long time.
#
# Dependencies:
# 1) A database named tpch1 containing 1 GB tpch tables.
#
# Arguments:
#   schema to run against (optional)

DB=tpch1
if [ -n "$1" ] ; then
        DB=$1
fi

./queryTester -q working_tpch1_compareLogOnly -u $DB -r Tr -g
