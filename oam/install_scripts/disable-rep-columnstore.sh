#!/bin/bash
#
# $Id$
#
# generic MariaDB Columnstore Disable Replication script.
#
# Notes: This script gets run by ProcMon:

# check log for error
checkForError() {
	grep ERROR /tmp/disable-rep-status.log > /tmp/error.check
	if [ `cat /tmp/error.check | wc -c` -ne 0 ]; then
		echo "ERROR: check log file: /tmp/disable-rep-status.log"
		rm -f /tmp/error.check
		exit 1
	fi
	rm -f /tmp/error.check
}

prefix=/usr/local
installdir=$prefix/mariadb/columnstore
pwprompt=
for arg in "$@"; do
	if [ `expr -- "$arg" : '--prefix='` -eq 9 ]; then
		prefix="`echo $arg | awk -F= '{print $2}'`"
		installdir=$prefix/mariadb/columnstore
	elif [ `expr -- "$arg" : '--password='` -eq 11 ]; then
		password="`echo $arg | awk -F= '{print $2}'`"
		pwprompt="--password=$password"
	elif [ `expr -- "$arg" : '--installdir='` -eq 13 ]; then
		installdir="`echo $arg | awk -F= '{print $2}'`"
		prefix=`dirname $installdir`
	fi
done

test -f $installdir/post/functions && . $installdir/post/functions

>/tmp/disable-rep-status.log

#
# Run stop slave command
#
echo "Run stop slave command" >>/tmp/disable-rep-status.log
cat >/tmp/idb_disable-rep.sql <<EOD
stop slave;
EOD

cat /tmp/idb_disable-rep.sql >>/tmp/disable-rep-status.log
$installdir/mysql/bin/mysql \
	--defaults-extra-file=$installdir/mysql/my.cnf \
	--user=root $pwprompt \
	calpontsys </tmp/idb_disable-rep.sql >>/tmp/disable-rep-status.log 2>&1

checkForError

#
# Run reset slave command
#
echo "Run reset slave command" >>/tmp/disable-rep-status.log
cat >/tmp/idb_disable-rep.sql <<EOD
reset slave;
EOD

cat /tmp/idb_disable-rep.sql >>/tmp/disable-rep-status.log
$installdir/mysql/bin/mysql \
	--defaults-extra-file=$installdir/mysql/my.cnf \
	--user=root $pwprompt \
	calpontsys </tmp/idb_disable-rep.sql >>/tmp/disable-rep-status.log 2>&1

checkForError

#alls good, 'OK' for success
echo "OK"
exit 0
