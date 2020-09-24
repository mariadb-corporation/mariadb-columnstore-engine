#!/bin/bash

# This script allows to gracefully start MCS

# prevent nodes using shared storage manager from stepping on each other when initializing
# flock will open up an exclusive file lock to run atomic operations
exec {fd_lock}>/var/lib/columnstore/storagemanager/storagemanager-lock
flock -x "$fd_lock"

/bin/systemctl start mcs-workernode
/bin/systemctl start mcs-controllernode
/bin/systemctl start mcs-primproc
/bin/systemctl start mcs-writeengineserver
/bin/systemctl start mcs-exemgr
/bin/systemctl start mcs-dmlproc
/bin/systemctl start mcs-ddlproc
sleep 2
su -s /bin/sh -c 'dbbuilder 7' mysql 1> /tmp/columnstore_tmp_files/dbbuilder.log

flock -u "$fd_lock"

exit 0
