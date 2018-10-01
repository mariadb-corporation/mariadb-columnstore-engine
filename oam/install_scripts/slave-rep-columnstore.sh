#!/bin/bash
#
# $Id$
#
# generic MariaDB Columnstore Slave Replication script.
#
# Notes: This script gets run by ProcMon during installs and upgrades:

# check log for error
checkForError() {
	grep ERROR ${tmpdir}/slave-rep-status.log > ${tmpdir}/error.check
	if [ `cat ${tmpdir}/error.check | wc -c` -ne 0 ]; then
		echo "ERROR: check log file: ${tmpdir}/slave-rep-status.log"
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
	elif [ `expr -- "$arg" : '--installdir='` -eq 13 ]; then
		installdir="`echo $arg | awk -F= '{print $2}'`"
		prefix=`dirname $installdir`
	elif [ `expr -- "$arg" : '--masteripaddr='` -eq 15 ]; then
		masteripaddr="`echo $arg | awk -F= '{print $2}'`"
	elif [ `expr -- "$arg" : '--masterlogfile='` -eq 16 ]; then
		masterlogfile="`echo $arg | awk -F= '{print $2}'`"
	elif [ `expr -- "$arg" : '--masterlogpos='` -eq 15 ]; then
		masterlogpos="`echo $arg | awk -F= '{print $2}'`"
	elif [ `expr -- "$arg" : '--port='` -eq 7 ]; then
		port="`echo $arg | awk -F= '{print $2}'`"
	elif [ $(expr -- "$arg" : '--tmpdir=') -eq 9 ]; then
		tmpdir="$(echo $arg | awk -F= '{print $2}')"
	fi
done

test -f $installdir/post/functions && . $installdir/post/functions

repUser="idbrep"
password="Calpont1"

>${tmpdir}/slave-rep-status.log

#
# Run stop slave command
#
echo "Run stop slave command" >>${tmpdir}/slave-rep-status.log
cat >${tmpdir}/idb_slave-rep.sql <<EOD
stop slave;
EOD

cat ${tmpdir}/idb_slave-rep.sql >>${tmpdir}/slave-rep-status.log
$installdir/mysql/bin/mysql \
	--defaults-extra-file=$installdir/mysql/my.cnf \
	--user=root \
	calpontsys <${tmpdir}/idb_slave-rep.sql >>${tmpdir}/slave-rep-status.log 2>&1

checkForError

#
# Run Change Master Command
#
echo "Run Change Master Command" >>${tmpdir}/slave-rep-status.log
cat >${tmpdir}/idb_slave-rep.sql <<EOD
CHANGE MASTER TO
    	MASTER_HOST='$masteripaddr',
    	MASTER_USER='$repUser',
    	MASTER_PASSWORD='$password',
    	MASTER_PORT=$port,
   	MASTER_LOG_FILE='$masterlogfile',
    	MASTER_LOG_POS=$masterlogpos;

EOD

cat ${tmpdir}/idb_slave-rep.sql >>${tmpdir}/slave-rep-status.log
$installdir/mysql/bin/mysql \
	--defaults-extra-file=$installdir/mysql/my.cnf \
	--user=root \
	calpontsys <${tmpdir}/idb_slave-rep.sql >>${tmpdir}/slave-rep-status.log 2>&1

checkForError

#
# Run start slave command
#
echo "Run start slave command" >>${tmpdir}/slave-rep-status.log
cat >${tmpdir}/idb_slave-rep.sql <<EOD
start slave;
EOD

cat ${tmpdir}/idb_slave-rep.sql >>${tmpdir}/slave-rep-status.log
$installdir/mysql/bin/mysql \
	--defaults-extra-file=$installdir/mysql/my.cnf \
	--user=root \
	calpontsys <${tmpdir}/idb_slave-rep.sql >>${tmpdir}/slave-rep-status.log 2>&1

checkForError

#
# Run SHOW SLAVE STATUS
#
echo "Run SHOW SLAVE STATUS to node log" >>${tmpdir}/slave-rep-status.log
cat >${tmpdir}/idb_slave-rep.sql <<EOD
SHOW SLAVE STATUS\G
EOD

cat ${tmpdir}/idb_slave-rep.sql >>${tmpdir}/slave-rep-status.log
$installdir/mysql/bin/mysql \
	--defaults-extra-file=$installdir/mysql/my.cnf \
	--user=root \
	calpontsys <${tmpdir}/idb_slave-rep.sql >>${tmpdir}/slave-rep-status.log 2>&1

checkForError

#alls good, 'OK' for success
echo "OK"
exit 0
