#!/bin/bash

DB=dml

if [ $# -eq 1 ]; then
	DB=$1
fi

# Clear out all logs.
rm -f logs/*.sql.log

# Start with negative test case of trying to create a varbinary column without the Calpont.xml setting.
$MYSQLCMD $DB -f < createn.sql > logs/createn.sql.log 2>&1

../../scripts/setConfig.sh WriteEngine AllowVarbinary yes

# Drop / create tables.
echo "Creating tables..."
$MYSQLCMD $DB -f < create.sql > logs/create.sql.log 2>&1

#
# Expand 500 rows into a million rows to import into the staging table.
#
rm -f temp2.tbl temp3.tbl
cp temp.tbl temp2.tbl
echo "Generating import file..."
cat temp2.tbl temp2.tbl temp2.tbl temp2.tbl temp2.tbl temp2.tbl temp2.tbl temp2.tbl temp2.tbl temp2.tbl > temp3.tbl 	# 5,000 rows now
cat temp3.tbl temp3.tbl temp3.tbl temp3.tbl temp3.tbl temp3.tbl temp3.tbl temp3.tbl temp3.tbl temp3.tbl > temp2.tbl 	# 50,000 rows now 
cat temp2.tbl temp2.tbl temp2.tbl temp2.tbl temp2.tbl > temp3.tbl 							# 250,000 rows now
cat temp3.tbl temp3.tbl temp3.tbl temp3.tbl > temp2.tbl 								# 1 mil rows now

#
# Import 2 mil rows into the fact table.
#
echo "Import into staging table..."
awk '{print NR "|" $0}' temp2.tbl | $CPIMPORTCMD $DB test012_staging
rm -f temp2.tbl
rm -f temp3.tbl

#
# Import 1 mil rows into the fact table.
#
echo "Import into fact table..."
echo "dummy" | awk '{for(i=1; i<=1000000; i++)print i "||||"}' | $CPIMPORTCMD $DB test012_fact

#
# Do some DML and DDL.
#
echo "Running DML and DDL script..."
$MYSQLCMD $DB -f -n < mods.sql > logs/mods.sql.log 2>&1

#
# Validate results.
#
echo "Running validation queries..."
$MYSQLCMD $DB -f -n < validate.sql > logs/validate.sql.log 2>&1
$MYSQLCMD $DB -f -n < bug3799.sql > logs/bug3799.sql.log 2>&1
$MYSQLCMD $DB -f -n < bug4374.sql > logs/bug4374.sql.log 2>&1

#
# Report status.
#
cat /dev/null > logs/diff.txt
for i in *sql; do
	diff -b logs/$i.ref.log logs/$i.log > /dev/null
	if [ $? -ne 0 ]; then
		echo "logs/$i.log does not match logs/$i.ref.log" >> logs/diff.txt
	fi
done
diffs=`wc -l logs/diff.txt | awk '{print $1}'`
if [ $diffs -gt 0 ]; then
	echo "Failed (Check logs/diff.txt for diffs)" > status.txt
	ret=1
else
	echo "Passed" > status.txt
	ret=0
fi

#
# Flip the varbinary flag to no.
#
../../scripts/setConfig.sh WriteEngine AllowVarbinary no

echo "All done."
exit $ret

