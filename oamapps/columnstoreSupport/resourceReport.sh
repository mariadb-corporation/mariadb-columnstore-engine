#! /bin/sh
#
# $Id: resourceReport.sh 421 2007-04-05 15:46:55Z dhill $
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
echo "******************** Resource Usage Report for ${MODULE} ********************"
echo " "

echo " "
echo "-- Shared Memory --"
echo " "
echo "################# ipcs -l #################"
echo " "
ipcs -l

echo "################# clearShm -n #################"
echo " "
clearShm -n

echo " "
echo "-- Disk Usage --"
echo " "
echo "################# df -k #################"
echo " "
df -k

echo " "
echo "-- Disk BRM Data files --"
echo " "
ls -l /var/lib/columnstore/data1/systemFiles/dbrm 2> /dev/null
ls -l /var/lib/columnstore/dbrm 2> /dev/null

echo "################# cat /var/lib/columnstore/data1/systemFiles/dbrm/BRM_saves_current #################"
echo " "
cat /var/lib/columnstore/data1/systemFiles/dbrm/BRM_saves_current 2> /dev/null

echo " "
echo "-- View Table Locks --"
echo " "
echo "################# cat bin/viewtablelock #################"
echo " "
viewtablelock 2> /dev/null

echo " "
echo "-- BRM Extent Map  --"
echo " "
echo "################# bin/editem -i #################"
echo " "
editem -i 2>/dev/null

} >> $OUT_FILE

exit 0
