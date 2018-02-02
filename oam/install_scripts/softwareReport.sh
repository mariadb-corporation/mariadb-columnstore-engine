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
        INSTALLDIR=$2
else
        INSTALLDIR="/usr/local/mariadb/columnstore"
fi

USER=`whoami 2>/dev/null`

if [ $USER = "root" ]; then
	SUDO=" "
else
	SUDO="sudo"
fi

$SUDO rm -f /tmp/${MODULE}_softwareReport.txt

{
echo " "
echo "******************** Software Report for ${MODULE} ********************"
echo " "

echo " "
echo "-- Columnstore Package Details --"
echo " "
echo "################# mcsadmin getcolumnstoresoftwareinfo #################"
echo " "
$INSTALLDIR/bin/mcsadmin getsoftwareinfo

echo " "
echo "-- Columnstore Storage Configuration --"
echo " "
echo "################# mcsadmin getStorageConfig #################"
echo " "
$INSTALLDIR/bin/mcsadmin getStorageConfig

} > /tmp/${MODULE}_softwareReport.txt

exit 0
