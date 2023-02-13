#!/usr/bin/env sh

set -x

SCRIPT_LOCATION=$(dirname "$0")
DIR_NAME=$1

journalctl -u mcs-ddlproc.service > "$DIR_NAME"/ddlproc.log
journalctl -u mcs-dmlproc.service > "$DIR_NAME"/dmlproc.log
journalctl -u mcs-loadbrm.service > "$DIR_NAME"/loadbrm.log
journalctl -u mcs-primproc.service > "$DIR_NAME"/primproc.log
journalctl -u mcs-workernode@1.service > "$DIR_NAME"/workernode.log
journalctl -u mcs-writeengineserver.service > "$DIR_NAME"/writeengineserver.log
