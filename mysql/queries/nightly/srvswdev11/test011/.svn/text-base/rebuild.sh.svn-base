#!/bin/bash
#
# Create tables used to test cpimport features.
#

DB=bulkload_features

$MYSQLCMD < create_tables.sql

PGMPATH=$INSTALLDIR/bin

if $WINDOWS; then
	BULKDATAPATH=$WINDRIVE/usr/local/Calpont/data/bulk/data/
else	
	BULKDATAPATH=/usr/local/Calpont/data/bulk/data/
fi

$PGMPATH/colxml -j 1111 -t saturation $DB
$PGMPATH/cpimport -j 1111 -f STDIN < tables/saturation.tbl

$PGMPATH/colxml -j 1112 -t autoinc $DB
$PGMPATH/cpimport -j 1112 -f STDIN < tables/autoinc.tbl

$PGMPATH/colxml -j 1113 -t enclosedby $DB
$PGMPATH/cpimport -j 1113 -E\" -f STDIN < tables/enclosedby.tbl

$PGMPATH/colxml -j 1114 -t bug3801 $DB
$PGMPATH/cpimport -j 1114 -f STDIN < tables/bug3801.tbl

# Test bug3810: read buffer copy overflow bug with embedded double quotes in enclosed-by string.
# Note the use of the smaller read buffer size for this test (-c 1000)
$PGMPATH/colxml -j 1115 -t bug3810 -c 1000 -E'"' $DB
$PGMPATH/cpimport -j 1115 -f STDIN < tables/bug3810.tbl

# Job file import without stdin.
# This test requires a ssb/1G part table in /usr/local/Calpont/data/bulk/data/import_local/ssb/1G
if $WINDOWS; then
	mv $BULKDATAPATH/import_local/ssb/1G/*.tbl $INSTALLDIR/bulk/data/import
else
	rm -rf $BULKDATAPATH/import
	ln -s $BULKDATAPATH/import_local/ssb/1G $BULKDATAPATH/import
fi	

# WWW 3/18/2013.  Moved the database parameter to the end for colxml calls for Window compatibility.
#                 Bug 5122 is open for the issue.
$PGMPATH/colxml -t part $DB 
$PGMPATH/cpimport -j 299
if $WINDOWS; then
	mv $INSTALLDIR/bulk/data/import/*.tbl $BULKDATAPATH/import_local/ssb/1G
fi

# @bug4286.  Select | import.  Uses tab separator and NULL as null option.
# This test requires the ssb/1G part table loaded by the previous test
$MYSQLCMD $DB -q --skip-column-names -e "select * from part;" | $PGMPATH/cpimport -s '\t' -n1 $DB selectIntoImport

# Simple usage.
# This test requires a 1G customer table in /usr/local/Calpont/data/bulk/data/import_local/ssb/1G
$PGMPATH/cpimport $DB simple $BULKDATAPATH/import_local/ssb/1G/customer.tbl

# Simple usage running the file from the local directory.
cd tables
$PGMPATH/cpimport $DB bug4231 bug4231.tbl
cd ..

# Test bug2828: add support for Not Null with default
rm -f tables/bug2828notnull.tbl.Job_1116*.bad*
rm -f tables/bug2828notnull.tbl.Job_1116*.err*
$PGMPATH/colxml -j 1116 -t bug2828notnull $DB
if $WINDOWS; then
	# Windows $PGM sometimes puts /cygwin/c/ in front. cpimport can't interpret this properly
	$PGMPATH/cpimport -j 1116 -e 100 $DB bug2828notnull /InfiniDB/src/mysql/queries/nightly/srvswdev11/test011/tables/bug2828notnull.tbl
else
	$PGMPATH/cpimport -m1 -j 1116 -e 100 $DB bug2828notnull $PWD/tables/bug2828notnull.tbl
fi

# Bug 4476, the -P parameter was causing a seg fault when the specified PM did have any extents.
if $WINDOWS; then
	echo "dummy" | awk '{for(i=1; i<=10; i++)print i "|" i "|"}' | $PGMPATH/cpimport -P 1 $DB misc
	echo "dummy" | awk '{for(i=1; i<=10; i++)print i "|" i "|"}' | $PGMPATH/cpimport -P 1 $DB misc2
else
	echo "dummy" | awk '{for(i=1; i<=10; i++)print i "|" i "|"}' | $PGMPATH/cpimport -P 1 -m 1 $DB misc
	echo "dummy" | awk '{for(i=1; i<=10; i++)print i "|" i "|"}' | $PGMPATH/cpimport -P 1 -m 1 $DB misc2
fi
# Bug 4171, support multiple table imports thru job files
$PGMPATH/colxml -j 4171 -t bug4171a -l $PWD/tables/bug4171a.tbl -t bug4171b -l $PWD/tables/bug4171b.tbl $DB
if $WINDOWS; then
	# Windows $PGM sometimes puts /cygwin/c/ in front. colxml can't interpret this properly
	TESTPATH=/InfiniDB/src/mysql/queries/nightly/srvswdev11/test011
	$PGMPATH/colxml -j 4171 -t bug4171a -l $TESTPATH/tables/bug4171a.tbl -t bug4171b -l $TESTPATH/tables/bug4171b.tbl $DB
	$PGMPATH/cpimport -j 4171
else
	$PGMPATH/colxml -j 4171 -t bug4171a -l $PWD/tables/bug4171a.tbl -t bug4171b -l $PWD/tables/bug4171b.tbl $DB
	$PGMPATH/cpimport -m 1 -j 4171
fi

# Bug 4089: correctly write bad rows with enclosedBy character
rm -f bug4089enclosedCharBadFile.tbl.Job_4089*.bad*
rm -f bug4089enclosedCharBadFile.tbl.Job_4089*.err*
rm -f tables/bug4089enclosedCharBadFile2.tbl
$PGMPATH/colxml -j 4089 -t bug4089enclosedCharBadFile $DB
cat tables/bug4089enclosedCharBadFile.tbl | $PGMPATH/cpimport -j 4089 -f STDIN -E '"'
# Add second column to *.bad file and retry the import
sed -e 's/$/|666/g' bug4089enclosedCharBadFile.tbl.Job_4089*.bad* > tables/bug4089enclosedCharBadFile2.tbl
$PGMPATH/cpimport -j 4089 -f STDIN -E '"' < tables/bug4089enclosedCharBadFile2.tbl

# Bug 4916: handle enclosed strings that cross read buffer boundary.
# Note special read buffer size of 275 bytes, to test a case where the enclosed
# string in record 11 crosses a read buffer boundary.
# Import data is a modeled after test case presented by Guavus in bug 4916.
if $WINDOWS; then
	$PGMPATH/cpimport  -c 275 -E \" -C \\ $DB bug4916enclosedbdry tables/bug4916enclosedbdry.tbl
else
	$PGMPATH/cpimport $DB bug4916enclosedbdry tables/bug4916enclosedbdry.tbl -c 275 -E '"' -C '\'
fi

# Bug 4342: support a list of files from command line (both mode 1 & 3)
cd tables
if $WINDOWS; then
	$PGMPATH/cpimport $DB bug4342 "bug4342a.tbl,bug4342b.tbl"
	$PGMPATH/cpimport $DB bug4342_m3 "bug4342a.tbl,bug4342b.tbl"
else
	$PGMPATH/cpimport -m 1 $DB bug4342 "bug4342a.tbl bug4342b.tbl"
	$PGMPATH/cpimport $DB bug4342_m3 "bug4342a.tbl bug4342b.tbl"
fi	
cd ..

# Bug 5027: test binary imports into a table having signed numeric columns
$PGMPATH/colxml   -j 1117 -t binarysigned $DB
$PGMPATH/cpimport -j 1117     -f STDIN < tables/binarytxtnum.tbl

$PGMPATH/colxml   -j 1118 -t binarysigned $DB
$PGMPATH/cpimport -j 1118 -I1 -f STDIN < tables/binary1signed.tbl

$PGMPATH/colxml   -j 1119 -t binarysigned $DB
$PGMPATH/cpimport -j 1119 -I2 -f STDIN < tables/binary2signed.tbl

# Bug 5027: test binary imports into a table having unsigned numeric columns
$PGMPATH/colxml   -j 1120 -t binaryunsigned $DB
$PGMPATH/cpimport -j 1120     -f STDIN < tables/binarytxtnum.tbl

$PGMPATH/colxml   -j 1121 -t binaryunsigned $DB
$PGMPATH/cpimport -j 1121 -I1 -f STDIN < tables/binary1unsigned.tbl

$PGMPATH/colxml   -j 1122 -t binaryunsigned $DB
$PGMPATH/cpimport -j 1122 -I2 -f STDIN < tables/binary2unsigned.tbl

# Bug 5027: test imports imports into a table having non-numeric columns
$PGMPATH/colxml   -j 1123 -t binarychar $DB
$PGMPATH/cpimport -j 1123     -f STDIN < tables/binarytxtchar.tbl

$PGMPATH/colxml   -j 1124 -t binarychar $DB
$PGMPATH/cpimport -j 1124 -I1 -f STDIN < tables/binary1char.tbl

$PGMPATH/colxml   -j 1125 -t binarychar $DB
$PGMPATH/cpimport -j 1125 -I1 -f STDIN < tables/binary2char.tbl

# Bug 4379: test -f parameter
rm -rf /tmp/bulkload_features
mkdir /tmp/bulkload_features
echo "" | awk '{for(i=1; i<=10; i++)print i}' > $WINDRIVE/tmp/bulkload_features/bug4379.tbl
$PGMPATH/colxml -j 1126 -t bug4379 $DB
$PGMPATH/cpimport -j 1126 -f $WINDRIVE/tmp/bulkload_features

