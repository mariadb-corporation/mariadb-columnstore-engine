#! /bin/sh
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
echo "******************** Hardware Report for ${MODULE} ********************"
echo " "

echo "-- Server OS Version --"
echo " "
echo "################# cat /proc/version #################"
echo " "
cat /proc/version 2>/dev/null
echo " "
echo "################# uname -a #################"
echo " "
uname -a
echo " "
echo "################# cat /etc/issue #################"
echo " "
cat /etc/issue 2>/dev/null
echo " "
echo "run columnstore_os_check.sh"
echo " "
echo "################# /bin/columnstore_os_check.sh #################"
echo " "
columnstore_os_check.sh 2>/dev/null

echo " "
echo "-- Server Uptime --"
echo " "
echo "################# uptime #################"
echo " "
uptime

echo " "
echo "-- Server cpu-info --"
echo " "
echo "################# cat /proc/cpuinfo #################"
echo " "
cat /proc/cpuinfo 2>/dev/null

echo " "
echo "-- Server memory-info --"
echo " "
echo "################# cat /proc/meminfo #################"
echo " "
$cat /proc/meminfo 2>/dev/null

echo " "
echo "-- Server mounts --"
echo " "
echo "################# cat /proc/mounts #################"
echo " "
cat /proc/mounts 2>/dev/null

echo " "
echo "-- Server Ethernet Configuration --"
echo " "
echo "################# ifconfig -a #################"
echo " "
ifconfig -a 2>/dev/null

} >> $OUT_FILE

exit 0
