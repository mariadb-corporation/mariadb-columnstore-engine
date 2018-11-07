#!/bin/bash
#
# $Id$
#
# generic MariaDB Columnstore Master Replication script.
#
# Notes: This script gets run by ProcMon during installs and upgrades:

# check log for error
checkForError() {
	grep ERROR ${tmpdir}/master-rep-status-$hostipaddr.log > ${tmpdir}/error.check
	if [ `cat ${tmpdir}/error.check | wc -c` -ne 0 ]; then
		echo "ERROR: check log file: ${tmpdir}/master-rep-status-$hostipaddr.log"
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
	elif [ `expr -- "$arg" : '--hostIP='` -eq 9 ]; then
		hostipaddr="`echo $arg | awk -F= '{print $2}'`"
	elif [ $(expr -- "$arg" : '--tmpdir=') -eq 9 ]; then
		tmpdir="$(echo $arg | awk -F= '{print $2}')"
	fi
done

test -f $installdir/post/functions && . $installdir/post/functions

repUser="idbrep"
password="Calpont1"

>${tmpdir}/master-rep-status-$hostipaddr.log

#
# Create Replication User
#
echo "Create Replication User $repUser for node $hostipaddr" >>${tmpdir}/master-rep-status-$hostipaddr.log
cat >${tmpdir}/idb_master-rep.sql <<EOD
CREATE USER IF NOT EXISTS '$repUser'@'$hostipaddr' IDENTIFIED BY '$password';
GRANT REPLICATION SLAVE ON *.* TO '$repUser'@'$hostipaddr';
EOD

cat ${tmpdir}/idb_master-rep.sql >>${tmpdir}/master-rep-status-$hostipaddr.log
$installdir/mysql/bin/mysql \
	--defaults-extra-file=$installdir/mysql/my.cnf \
	--user=root \
	calpontsys <${tmpdir}/idb_master-rep.sql >>${tmpdir}/master-rep-status-$hostipaddr.log 2>&1

checkForError

#
# Grant table access for created user
#
echo "Grant table access for $repUser for node $hostipaddr" >>${tmpdir}/master-rep-status-$hostipaddr.log
cat >${tmpdir}/idb_master-rep.sql <<EOD
use mysql
grant all on *.* to '$repUser'@'$hostipaddr' identified by 'Calpont1';
grant REPLICATION SLAVE on *.* to '$repUser'@'$hostipaddr' identified by 'Calpont1';
EOD

cat ${tmpdir}/idb_master-rep.sql >>${tmpdir}/master-rep-status-$hostipaddr.log
$installdir/mysql/bin/mysql \
	--defaults-extra-file=$installdir/mysql/my.cnf \
	--user=root \
	calpontsys <${tmpdir}/idb_master-rep.sql >>${tmpdir}/master-rep-status-$hostipaddr.log 2>&1

checkForError

#
# Run SHOW MASTER STATUS
#
echo "Run SHOW MASTER STATUS to node log" >>${tmpdir}/master-rep-status-$hostipaddr.log
cat >${tmpdir}/idb_master-rep.sql <<EOD
SHOW MASTER STATUS
EOD

cat ${tmpdir}/idb_master-rep.sql >>${tmpdir}/master-rep-status-$hostipaddr.log
$installdir/mysql/bin/mysql \
	--defaults-extra-file=$installdir/mysql/my.cnf \
	--user=root \
	calpontsys <${tmpdir}/idb_master-rep.sql >>${tmpdir}/master-rep-status-$hostipaddr.log 2>&1

checkForError

echo "Run SHOW MASTER STATUS to master status log ${tmpdir}/show-master-status.log" >>${tmpdir}/master-rep-status-$hostipaddr.log
cat >${tmpdir}/idb_master-rep.sql <<EOD
SHOW MASTER STATUS
EOD

cat ${tmpdir}/idb_master-rep.sql >${tmpdir}/show-master-status.log
$installdir/mysql/bin/mysql \
	--defaults-extra-file=$installdir/mysql/my.cnf \
	--user=root \
	calpontsys <${tmpdir}/idb_master-rep.sql >>${tmpdir}/show-master-status.log


#alls good, 'OK' for success
echo "OK"
exit 0


