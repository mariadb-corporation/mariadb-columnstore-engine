#! /bin/sh
#
# $Id: logReport.sh 421 2007-04-05 15:46:55Z dhill $
#
if [ $1 ] ; then
        SERVER=$1
else
        SERVER="localhost"
fi

if [ $2 ] ; then
        DATE=$2
else
        DATE=" "
fi

#get temp directory
tmpDir=`mcsGetConfig SystemConfig SystemTempFileDir`

rm -f ${tmpDir}/logReport.log

{
echo " "
echo "******************** Alarm Report for $SERVER ********************"
echo " "

echo "-- Today's Alarms --"
echo " "
cat /var/log/mariadb/columnstore/alarm.log 2>/dev/null

if test -f /var/log/mariadb/columnstore/archive/alarm.log-$DATE ; then
	echo "-- Archived Alarms --"
	echo " "
	cat /var/log/mariadb/columnstore/archive/alarm.log-$DATE 2>/dev/null
fi

} > ${tmpDir}/logReport.log

exit 0
