#!/bin/bash

#
# Parms:
#    numeric id (required)
#    database (optional) defaults to tpch1 if not provided.

session=$1
db=tpch1

if [ $# -gt 1 ]
then
	db=$2
fi

rm -rf $1
mkdir $1

for i in `ls *.sql`
do
echo Session $session is running $i
$MYSQLCMD $db < $i > ./$1/$i.log 2>&1
done

echo Session $session completed.

