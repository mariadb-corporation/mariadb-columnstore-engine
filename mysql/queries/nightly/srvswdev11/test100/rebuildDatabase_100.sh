#!/bin/bash

TPCHDB=tpch100
INSTALLDIR=/usr/local/Calpont
CPIMPORTCMD=/usr/local/Calpont/bin/cpimport

# Create the necessary schemas.
$MYSQLCMD -e "create database if not exists $TPCHDB;"

# Create the $TPCHDB tables.
echo "Creating tpch tables in $TPCHDB."
$MYSQLCMD $TPCHDB < /root/genii/mysql/scripts/create_tpch_8byte.sql

# Import the 100 GB tpch tables.
echo ""
echo "Importing into $TPCHDB tables."
#rm -rf /usr/local/Calpont/data/bulk/data/import
#ln -s /usr/local/Calpont/data/bulk/data/import_local/tpch/1g /usr/local/Calpont/data/bulk/data/import
IMPORTDIR=/usr/local/Calpont/data/bulk/data/import_local/tpch/1g

$INSTALLDIR/bin/colxml -t region -t nation -t customer -t part -t supplier -t partsupp -t orders -t lineitem $TPCHDB 

$CPIMPORTCMD -j 299 -f $IMPORTDIR
$CPIMPORTCMD -j 299 -f $IMPORTDIR
$CPIMPORTCMD -j 299 -f $IMPORTDIR
$CPIMPORTCMD -j 299 -f $IMPORTDIR
$CPIMPORTCMD -j 299 -f $IMPORTDIR
$CPIMPORTCMD -j 299 -f $IMPORTDIR
$CPIMPORTCMD -j 299 -f $IMPORTDIR
$CPIMPORTCMD -j 299 -f $IMPORTDIR
$CPIMPORTCMD -j 299 -f $IMPORTDIR
$CPIMPORTCMD -j 299 -f $IMPORTDIR

echo "All done."
