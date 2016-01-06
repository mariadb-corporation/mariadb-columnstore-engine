# Runs the sql files in the working_ssb_compareLogOnly directory.
#
# Notes:
# 1) Runs queries against the ssb schema.
# 2) Compares to checked in reference files.  Running these scripts against the reference database takes many hours.
#
# Dependencies:
# 1) A database named ssb containing the ssb tables populated with 1GB data.
# 
# Arguments:
#   schema to run against (optional)

DB=ssb
if [ -n "$1" ] ; then
	DB=$1
fi

./queryTester -q working_ssb_compareLogOnly -r Tr -u $DB -g
