#!/bin/bash
#
# $Id$
#
# generic MariaDB Columnstore Master Replication script.
#
# Notes: This script gets run by ProcMon during installs and upgrades:

# check log for error
checkForError() {
	grep ERROR /tmp/master-rep-status-$hostipaddr.log > /tmp/error.check
	if [ `cat /tmp/error.check | wc -c` -ne 0 ]; then
		echo "ERROR: check log file: /tmp/master-rep-status-$hostipaddr.log"
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
	elif [ `expr -- "$arg" : '--installdir='` -eq 13 ]; then
		installdir="`echo $arg | awk -F= '{print $2}'`"
		prefix=`dirname $installdir`
	elif [ `expr -- "$arg" : '--hostIP='` -eq 9 ]; then
		hostipaddr="`echo $arg | awk -F= '{print $2}'`"
	fi
done

test -f $installdir/post/functions && . $installdir/post/functions

repUser="idbrep"
password="Calpont1"

>/tmp/master-rep-status-$hostipaddr.log

#
# Create Replication User
#
echo "Create Replication User $repUser for node $hostipaddr" >>/tmp/master-rep-status-$hostipaddr.log
cat >/tmp/idb_master-rep.sql <<EOD
CREATE USER IF NOT EXISTS '$repUser'@'$hostipaddr' IDENTIFIED BY '$password';
GRANT REPLICATION SLAVE ON *.* TO '$repUser'@'$hostipaddr';
EOD

cat /tmp/idb_master-rep.sql >>/tmp/master-rep-status-$hostipaddr.log
$installdir/mysql/bin/mysql \
	--defaults-extra-file=$installdir/mysql/my.cnf \
	--user=root \
	calpontsys </tmp/idb_master-rep.sql >>/tmp/master-rep-status-$hostipaddr.log 2>&1

checkForError

#
# Grant table access for created user
#
echo "Grant table access for $repUser for node $hostipaddr" >>/tmp/master-rep-status-$hostipaddr.log
cat >/tmp/idb_master-rep.sql <<EOD
use mysql
grant all on *.* to '$repUser'@'$hostipaddr' identified by 'Calpont1';
grant REPLICATION SLAVE on *.* to '$repUser'@'$hostipaddr' identified by 'Calpont1';
EOD

cat /tmp/idb_master-rep.sql >>/tmp/master-rep-status-$hostipaddr.log
$installdir/mysql/bin/mysql \
	--defaults-extra-file=$installdir/mysql/my.cnf \
	--user=root \
	calpontsys </tmp/idb_master-rep.sql >>/tmp/master-rep-status-$hostipaddr.log 2>&1

checkForError

#
# Run SHOW MASTER STATUS
#
echo "Run SHOW MASTER STATUS to node log" >>/tmp/master-rep-status-$hostipaddr.log
cat >/tmp/idb_master-rep.sql <<EOD
SHOW MASTER STATUS
EOD

cat /tmp/idb_master-rep.sql >>/tmp/master-rep-status-$hostipaddr.log
$installdir/mysql/bin/mysql \
	--defaults-extra-file=$installdir/mysql/my.cnf \
	--user=root \
	calpontsys </tmp/idb_master-rep.sql >>/tmp/master-rep-status-$hostipaddr.log 2>&1

checkForError

echo "Run SHOW MASTER STATUS to master status log /tmp/show-master-status.log" >>/tmp/master-rep-status-$hostipaddr.log
cat >/tmp/idb_master-rep.sql <<EOD
SHOW MASTER STATUS
EOD

cat /tmp/idb_master-rep.sql >/tmp/show-master-status.log
$installdir/mysql/bin/mysql \
	--defaults-extra-file=$installdir/mysql/my.cnf \
	--user=root \
	calpontsys </tmp/idb_master-rep.sql >>/tmp/show-master-status.log


#alls good, 'OK' for success
echo "OK"
exit 0


