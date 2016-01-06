#!/bin/bash

#
# Functions that make mysql calls used by some of the nightly test scripts.
#

#
# This function outputs the count from the passed sql statement.
# Parameters:
# db  - the database
# sql - the sql statement
#
# Example:
# liCount=`getCount tpch1 "select count(*) from lineitem;"`
#
getCount()
{
        # The awk in the result is a hack for cygwin to avoid arithmetic expression errors and jumbled wrapping in the status
        # output.
	db=$1
        sql=$2
        $MYSQLCMD $db -e "$sql" | tail -1 | awk '{print $0}'
}

