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

rm -f /tmp/${MODULE}_logReport.tar.gz

tar -zcf /tmp/${MODULE}_logReport.tar.gz /var/log/mariadb/columnstore > /dev/null 2>&1

exit 0
