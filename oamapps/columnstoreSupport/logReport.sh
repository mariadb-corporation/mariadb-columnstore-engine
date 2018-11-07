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
        INSTALLDIR=$2
else
        INSTALLDIR="/usr/local/mariadb/columnstore"
fi

#get temp directory
tmpDir=`$INSTALLDIR/bin/getConfig SystemConfig SystemTempFileDir`

rm -f ${tmpDir}/${MODULE}_logReport.tar.gz

tar -zcf ${tmpDir}/${MODULE}_logReport.tar.gz /var/log/mariadb/columnstore > /dev/null 2>&1

exit 0
