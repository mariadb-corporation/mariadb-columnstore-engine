#!/bin/bash
#
# $Id$
#
# generic MariaDB Columnstore Disable Replication script.
#
# Notes: This script gets run by ProcMon:

# check log for error
checkForError() {
	grep ERROR ${tmpdir}/disable-rep-status.log > ${tmpdir}/error.check
	if [ `cat ${tmpdir}/error.check | wc -c` -ne 0 ]; then
		echo "ERROR: check log file:${tmpdir}/disable-rep-status.log"
		rm -f ${tmpdir}/error.check
		exit 1
	fi
	rm -f ${tmpdir}/error.check
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
	elif [ $(expr -- "$arg" : '--tmpdir=') -eq 9 ]; then
		tmpdir="$(echo $arg | awk -F= '{print $2}')"
	fi
done

test -f $installdir/post/functions && . $installdir/post/functions

>${tmpdir}/disable-rep-status.log

#
# Run stop slave command
#
echo "Run stop slave command" >>${tmpdir}/disable-rep-status.log
cat >${tmpdir}/idb_disable-rep.sql <<EOD
stop slave;
EOD

cat ${tmpdir}/idb_disable-rep.sql >>${tmpdir}/disable-rep-status.log
$installdir/mysql/bin/mysql \
	--defaults-extra-file=$installdir/mysql/my.cnf \
	--user=root $pwprompt \
	calpontsys <${tmpdir}/idb_disable-rep.sql >>${tmpdir}/disable-rep-status.log 2>&1

checkForError

#
# Run reset slave command
#
echo "Run reset slave command" >>${tmpdir}/disable-rep-status.log
cat >${tmpdir}/idb_disable-rep.sql <<EOD
reset slave;
EOD

cat ${tmpdir}/idb_disable-rep.sql >>${tmpdir}/disable-rep-status.log
$installdir/mysql/bin/mysql \
	--defaults-extra-file=$installdir/mysql/my.cnf \
	--user=root $pwprompt \
	calpontsys <${tmpdir}/idb_disable-rep.sql >>${tmpdir}/disable-rep-status.log 2>&1

checkForError

#alls good, 'OK' for success
echo "OK"
exit 0
