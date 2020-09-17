#!/bin/bash

# This script allows to gracefully start MCS

IFLAG=/var/lib/columnstore/storagemanager/cs-initialized

/bin/systemctl start mcs-workernode
/bin/systemctl start mcs-controllernode
/bin/systemctl start mcs-primproc
/bin/systemctl start mcs-writeengineserver
/bin/systemctl start mcs-exemgr
/bin/systemctl start mcs-dmlproc
/bin/systemctl start mcs-ddlproc
sleep 2

# prevent nodes using shared storage manager from stepping on each other when starting up
if [ ! -e $IFLAG ]; then
    su -s /bin/sh -c 'dbbuilder 7' mysql 2> /tmp/columnstore_tmp_files/dbbuilder.log
    touch $IFLAG
fi

exit 0
