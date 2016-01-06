#!/bin/bash

#
# Perform miscellaneous tests.
#

DB=dmlc

cat /dev/null > diff.txt

$MYSQLCMD $DB -e "create database if not exists $DB;"

# Bug 5117.  Run query to validate rows affected.
$MYSQLCMD $DB -vv < rowsAffected.sql > rowsAffected.sql.log 2>&1
# Bug 5315.  Run query to validate rows affected.
$MYSQLCMD $DB -vv < bug5315.sql > bug5315.sql.log 2>&1

# Diff with the ref log.  Return 0 if matches, 1 otherwise.
diff -b rowsAffected.sql.ref.log rowsAffected.sql.log > /dev/null
ret=$?
if [ $ret -ne 0 ]; then
	echo "rowsAffected.sql.ref.log does not match rowsAffected.sql.log." >> diff.txt
fi
diff -b bug5315.sql.ref.log bug5315.sql.log > /dev/null
ret=$?
if [ $ret -ne 0 ]; then
        echo "bug5315.sql.ref.log does not match bug5315.sql.log." >> diff.txt
fi

exit $ret
