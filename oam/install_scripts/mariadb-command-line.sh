#!/bin/bash
#
# $Id$
#
# generic MariaDB Columnstore Command Line script.
#
# Notes: This script gets run by ProcMon during installs and upgrades:

# check log for error
checkForError() {
	grep "ERROR 1045" /tmp/mariadb-command-line.log > /tmp/error.check
	if [ `cat /tmp/error.check | wc -c` -ne 0 ]; then
		echo "ERROR - PASSWORD: check log file: /tmp/mariadb-command-line.log"
		rm -f /tmp/error.check
		exit 2
	fi
	
	grep ERROR /tmp/mariadb-command-line.log > /tmp/error.check
	if [ `cat /tmp/error.check | wc -c` -ne 0 ]; then
		echo "ERROR: check log file: /tmp/mariadb-command-line.log"
		rm -f /tmp/error.check
		exit 1
	fi
	rm -f /tmp/error.check
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
	fi
done

test -f $installdir/post/functions && . $installdir/post/functions


>/tmp/mariadb-command-line.log

#
# Run command
#
echo "Run command" >>/tmp/mariadb-command-line.log
cat >/tmp/mariadb-command-line.sql <<EOD
$command;
EOD

cat /tmp/mariadb-command-line.sql >> /tmp/mariadb-command-line.log
$installdir/mysql/bin/mysql \
	--defaults-extra-file=$installdir/mysql/my.cnf \
	--user=root \
	calpontsys < /tmp/mariadb-command-line.sql >> /tmp/mariadb-command-line.log 2>&1

checkForError

#alls good, 'OK' for success
echo "OK"
exit 0
