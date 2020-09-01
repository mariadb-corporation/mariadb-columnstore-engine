#!/bin/bash

# This script allows to gracefully start MCS

/bin/systemctl start mcs-workernode
/bin/systemctl start mcs-controllernode
/bin/systemctl start mcs-primproc
/bin/systemctl start mcs-writeengineserver
/bin/systemctl start mcs-exemgr
/bin/systemctl start mcs-dmlproc
/bin/systemctl start mcs-ddlproc

su -s /bin/sh -c 'dbbuilder 7' mysql 2> /tmp/columnstore_tmp_files/dbbuilder.log

exit 0
