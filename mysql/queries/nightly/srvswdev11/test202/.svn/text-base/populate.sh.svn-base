#!/bin/bash

$MYSQLCMD -vvv < create.sql

#
# Populate wide table.
#
echo "dummy" | awk '{for (i=1; i<=100000; i++){k="";for (j=1; j<=51; j++){k=k i "|"}print k}}' | $CPIMPORTCMD wide wide

#
# Populate wide2 table.
#
echo "dummy" | awk '{for(i=1; i<=5000000; i++)print i "|1.22|2.32|4.23|9.88|7.22|9.88|7.32|23423.23" i "|" i "|" i "|" i "|" i "|" i "|" i "|" i "|" i "|" i "|" i "|" i "|" i "|" i "|"}' | $CPIMPORTCMD wide wide2

./populate_bug3515.sh
