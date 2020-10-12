#!/bin/bash

# This script allows to gracefully shut down MCS

/bin/systemctl stop mcs-dmlproc
/bin/systemctl stop mcs-ddlproc
/bin/systemctl stop mcs-exemgr
/bin/systemctl stop mcs-writeengineserver
/bin/systemctl stop mcs-primproc
/bin/systemctl stop mcs-controllernode
/bin/systemctl stop mcs-workernode@1.service
/bin/systemctl stop mcs-storagemanager

exit 0
