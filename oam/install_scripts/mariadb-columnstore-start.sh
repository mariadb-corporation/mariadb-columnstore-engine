#!/bin/bash

# This script allows to gracefully start MCS

# prevent nodes using shared storage manager from stepping on each other when initializing
# flock will open up an exclusive file lock to run atomic operations
exec {sm_lock}>/var/lib/columnstore/storagemanager/storagemanager-lock
flock -n "$sm_lock" || exit 0

# shared dbroot1 synchronization
# prevents dbbuilder from processing simultaneously on two or more nodes
exec {dbroot_lock}>/var/lib/columnstore/data1/dbroot1-lock
flock -x "$dbroot_lock"

/bin/systemctl start mcs-workernode
/bin/systemctl start mcs-controllernode
/bin/systemctl start mcs-primproc
/bin/systemctl start mcs-writeengineserver
/bin/systemctl start mcs-exemgr
/bin/systemctl start mcs-dmlproc
/bin/systemctl start mcs-ddlproc
sleep 2
su -s /bin/sh -c 'dbbuilder 7' mysql 1> /tmp/columnstore_tmp_files/dbbuilder.log

flock -u "$dbroot_lock"
flock -u "$sm_lock"

exit 0
