#!/bin/bash
#
# $Id$
#
# generic MariaDB Columnstore Command Line script.
#
# Notes: This script gets run by ProcMon during installs and upgrades:

# check log for error
checkForError() {
	grep "ERROR 1045" ${tmpdir}/mariadb-command-line.log > ${tmpdir}/error.check
	if [ `cat ${tmpdir}/error.check | wc -c` -ne 0 ]; then
		echo "ERROR - PASSWORD: check log file: ${tmpdir}/mariadb-command-line.log"
		rm -f ${tmpdir}/error.check
		exit 2
	fi
	
	grep ERROR ${tmpdir}/mariadb-command-line.log > ${tmpdir}/error.check
	if [ `cat ${tmpdir}/error.check | wc -c` -ne 0 ]; then
		echo "ERROR: check log file: ${tmpdir}/mariadb-command-line.log"
		rm -f ${tmpdir}/error.check
		exit 1
	fi
	rm -f ${tmpdir}/error.check
}

prefix=/usr/local
installdir=$prefix/mariadb/columnstore
pwprompt=
for arg in "$@"; do
	if [ `expr -- "$arg" : '--command='` -eq 10 ]; then
		command1="`echo $arg | awk -F= '{print $2}'`"
		command2="`echo $arg | awk -F= '{print $3}'`"
		command=$command1"="$command2
	elif [ `expr -- "$arg" : '--installdir='` -eq 13 ]; then
		installdir="`echo $arg | awk -F= '{print $2}'`"
		prefix=`dirname $installdir`
	elif [ `expr -- "$arg" : '--port='` -eq 7 ]; then
		port="`echo $arg | awk -F= '{print $2}'`"
	elif [ $(expr -- "$arg" : '--tmpdir=') -eq 9 ]; then
		tmpdir="$(echo $arg | awk -F= '{print $2}')"
	fi
done

test -f $installdir/post/functions && . $installdir/post/functions


>${tmpdir}/mariadb-command-line.log

#
# Run command
#
echo "Run command" >>${tmpdir}/mariadb-command-line.log
cat >${tmpdir}/mariadb-command-line.sql <<EOD
$command;
EOD

cat${tmpdir}/mariadb-command-line.sql >> ${tmpdir}/mariadb-command-line.log
$installdir/mysql/bin/mysql \
	--defaults-extra-file=$installdir/mysql/my.cnf \
	--user=root \
	calpontsys < ${tmpdir}/mariadb-command-line.sql >> ${tmpdir}/mariadb-command-line.log 2>&1

checkForError

#alls good, 'OK' for success
echo "OK"
exit 0
