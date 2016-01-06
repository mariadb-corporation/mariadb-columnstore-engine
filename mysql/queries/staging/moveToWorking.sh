#!/bin/bash

#
# This script moves the given sql file to the working folder by doing the following:
# 1) Moves the sql file to the working folder through svn.
# 2) Moves the .sql.ref.log file to the working folder through svn.
# 3) Replaces the .sq..ref.log file with the InfiniDB generated log (.sql.log).  
#
# It does not commit the changes.

if [ $# -lt 1 ]; then
	echo "Pass the sql file as a parameter."
	exit 1
fi

fl=$1

WORKING=/root/genii/mysql/queries/working_tpch1_compareLogOnly/windowFunctions/

svn revert $fl.ref.log > /dev/null
svn move $fl $WORKING/. 
svn move $fl.ref.log $WORKING/.
mv $fl.log $WORKING/$fl.ref.log

svn commit $fl.ref.log $fl $WORKING/$fl.ref.log $WORKING/$fl -m "Moved $fl windowFunction test case from staging to working."
