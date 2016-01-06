#!/bin/bash
DB=dmlc

if $WINDOWS; then
	FL=my.ini
else
	FL=my.cnf
fi

rm -f $FL*
rm -f Calpont.xml*
rm -f status.txt

#
# This function sets up the system for Japanese by making the config changes below and restarting the system.
# 1) Adds default-character-set=utf8 in the [client] section of the my.cnf file (Linux) or my.ini file (Windows).
# 2) Sets the SystemLang to ja_JP.UTF-8 in Calpont.xml.
#
turnOnJapanese() {

	#	
	# Add "default-character-set=utf8" to the [client] section of the MySQL config file.
	#
        cp $MYSQLCNF $FL.1
        linesAbove=`grep -n "\[client\]" $MYSQLCNF | awk -F ":" '{print $1}'`
        totalLines=`wc -l $MYSQLCNF | awk '{print $1}'`
        let linesBelow=totalLines-linesAbove
        head -$linesAbove $MYSQLCNF > $FL.2
        echo "default-character-set=utf8" >> $FL.2
        tail -$linesBelow $MYSQLCNF >> $FL.2
	cp $FL.2 $MYSQLCNF

	#
	# Change the SystemLang to utf8.
	#
	cp $INSTALLDIR/etc/Calpont.xml Calpont.xml.1
	../../scripts/setConfig.sh SystemConfig SystemLang ja_JP.UTF-8
	../../scripts/restart.sh
	cp $INSTALLDIR/etc/Calpont.xml Calpont.xml.2
}

#
# This function undoes the Japanse setting by:
# 1) Removes the default-character-set=utf8 from the MySQL config file.
# 2) Sets the SystemLang to C in Calpont.xml.
#
turnOffJapanese() {
	cp $FL.1 $MYSQLCNF
	../../scripts/setConfig.sh SystemConfig SystemLang C
	../../scripts/restart.sh
	cp $MYSQLCNF $FL.3
	cp $INSTALLDIR/etc/Calpont.xml Calpont.xml.3
}

cat /dev/null > diff.txt

echo "Configuring for Japanese."
rtn=0

#
# Configure for Japanese and restrt.
#
turnOnJapanese

#
# Create and populate the idbstrcoll table.
#
$MYSQLCMD $DB -e "create database if not exists $DB;"
$MYSQLCMD $DB < create_idbstrcoll.sql > create_idbstrcoll.sql.log 2>&1
$CPIMPORTCMD -s ',' -E\" $DB idbstrcoll idbstrcoll.csv > idbstrcoll.import.log 2>&1

#
# Run the .sql scripts in the sql folder and compare the results to the reference logs.
#
matched=0
misMatched=0
for i in sql/*.sql; do
	echo "Running $i."
	$MYSQLCMD $DB -n < $i > $i.log 2>&1

	#
	# If the results differ from the reference log, add a line to the diff.txt file.
	#
	if $WINDOWS; then
		if [ -f $i.win.ref.log ]; then
			diff -b $i.win.ref.log $i.log > /dev/null
		else
			diff -b $i.ref.log $i.log > /dev/null
		fi
	else
		diff -b $i.ref.log $i.log > /dev/null
	fi
	if [ $? -eq 0 ]; then
		let matched++;
	else
		rtn=1
		let misMatched++;
		echo "$i.log does not match $i.ref.log" >> diff.txt
		echo "-- Results did not match."
	fi
done

if [ $misMatched -gt 0 ] || [ $matched -eq 0 ]; then
	echo "Failed ($matched scripts matched, $misMatched did not match)" > status.txt
else
	echo "Passed ($matched scripts all matched)" > status.txt
fi

#
# Set configuration back as before and restart.
#
echo "Resetting configuration."
turnOffJapanese

exit $rtn
