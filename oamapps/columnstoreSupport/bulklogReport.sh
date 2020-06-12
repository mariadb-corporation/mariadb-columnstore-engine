#! /bin/sh
#
# $Id: logReport.sh 421 2007-04-05 15:46:55Z dhill $
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

if test -d /var/lib/columnstore/data/bulk ; then
	echo " "
	echo "-- Check for Errors in Bulk Logs --"
	echo " "
	echo "################# egrep '(ERR|CRIT)' /var/lib/columnstore/data/bulk/log/*.err #################"
	echo " "
	egrep '(ERR|CRIT)' /var/lib/columnstore/data/bulk/log/*.err 2>/dev/null
fi

} >> $OUT_FILE

exit 0
