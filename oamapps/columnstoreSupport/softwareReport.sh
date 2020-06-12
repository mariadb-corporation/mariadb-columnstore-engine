#!/bin/bash
#
# $Id: hardwareReport.sh 421 2007-04-05 15:46:55Z dhill $
#
if [ $1 ] ; then
        MODULE=$1
else
        MODULE="pm1"
fi

if [ $2 ] ; then
        OUT_FILE=$2
else
        OUT_FILE=${MODULE}_logReport.txt
fi

{
echo " "
echo "******************** Software Report for ${MODULE} ********************"
echo " "

echo " "
echo "-- Columnstore Package Details --"
echo " "
rpm -qi MariaDB-columnstore-engine
echo " "

} >> $OUT_FILE

exit 0
