#!/bin/bash
#
# $Id: hardwareReport.sh 421 2007-04-05 15:46:55Z dhill $
#
if [ $1 ] ; then
        MODULE=$1
else
        MODULE="pm1"
fi

#get temp directory
tmpDir=`mcsGetConfig SystemConfig SystemTempFileDir`

rm -f ${tmpDir}/${MODULE}_softwareReport.txt

{
echo " "
echo "******************** Software Report for ${MODULE} ********************"
echo " "

echo " "
echo "-- Columnstore Package Details --"
echo " "
echo "################# mcsadmin getcolumnstoresoftwareinfo #################"
echo " "
mcsadmin getsoftwareinfo

echo " "
echo "-- Columnstore Storage Configuration --"
echo " "
echo "################# mcsadmin getStorageConfig #################"
echo " "
mcsadmin getStorageConfig

} > ${tmpDir}/${MODULE}_softwareReport.txt

exit 0
