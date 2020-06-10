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
echo "******************** Configuration/Status Report for ${MODULE} ********************"
echo " "

chkconfig=`which chkconfig 2>/dev/null`
if [ -n "$chkconfig" ]; then
	echo "-- chkconfig configuration --"
	echo " "
	echo "################# chkconfig --list | grep columnstore #################"
	echo " "
	chkconfig --list | grep columnstore 2>/dev/null
fi

systemctl=`which systemctl 2>/dev/null`
if [ -n "$systemctl" ]; then
	echo "-- systemctl configuration --"
	echo " "
	echo "################# systemctl list-unit-files --type=service | grep columnstore #################"
	echo " "
	systemctl list-unit-files --type=service | grep columnstore 2>/dev/null
	echo "################# systemctl list-unit-files --type=service | grep mariadb #################"
	echo " "
	systemctl list-unit-files --type=service | grep mariadb 2>/dev/null
fi

updaterc=`which update-rc.d 2>/dev/null`
if [ -n "$updaterc" ]; then
	echo "-- services configuration --"
	echo " "
	echo "################# service --status-all | grep columnstore #################"
	echo " "
	service --status-all | grep columnstore 2>/dev/null
fi


echo " "
echo "-- fstab Configuration --"
echo " "
echo "################# cat /etc/fstab #################"
echo " "
cat /etc/fstab 2>/dev/null

echo " "
echo "-- Server Processes --"
echo " "
echo "################# ps axu #################"
echo " "
ps axu

echo " "
echo "-- Server Processes with resource usage --"
echo " "
echo "################# top -b -n 1 #################"
echo " "
top -b -n 1

} >> $OUT_FILE

exit 0
