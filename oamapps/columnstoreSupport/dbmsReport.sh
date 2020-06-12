#! /bin/sh
#
# $Id: dbmsReport.sh 
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

if [ $3 ] ; then
        MCSSUPPORTDIR=$3
else
        MCSSUPPORTDIR="/usr/share/columnstore"
fi

if [ $4 ] ; then
        PW_PROMPT=$4
else
        PW_PROMPT=""
fi

{

columnstoreMysql="mysql -u root ${PW_PROMPT} "

if ${columnstoreMysql} -V > /dev/null 2>&1; then
    echo " "
    echo "******************** DBMS Columnstore Version  *********************************"
    echo " "
    ${columnstoreMysql} -e 'status'
    echo " "
    echo "******************** DBMS Columnstore System Column  ***************************"
    echo " "
    ${columnstoreMysql} -e 'desc calpontsys.syscolumn;'
    echo " "
    echo "******************** DBMS Columnstore System Table  ****************************"
    echo " "
    ${columnstoreMysql} -e 'desc calpontsys.systable;'
    echo " "
    echo "******************** DBMS Columnstore System Catalog Data  *********************"
    echo " "
    ${columnstoreMysql}  calpontsys < $MCSSUPPORTDIR/dumpcat_mysql.sql
    echo " "
    echo "******************** DBMS Columnstore System Table Data  ***********************"
    echo "******************** DBMS Columnstore Databases  *******************************"
    echo " "
    ${columnstoreMysql} -e 'show databases;'
    echo " "
    echo "******************** DBMS Columnstore variables  *******************************"
    echo " "
    ${columnstoreMysql}  -e 'show variables;'
    echo " "
fi
} >> $OUT_FILE

exit 0

