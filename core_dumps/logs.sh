#!/usr/bin/env sh

set -x

SCRIPT_LOCATION=$(dirname "$0")
STEP_NAME=$1
DIR_NAME="unit_logs"

mkdir "$DIR_NAME"

dump_log ()
{
    name=$1
    journalctl -u "$name".service > "$DIR_NAME"/${name}_${STEP_NAME}".log
}

dump_log "mcs-ddlproc"
dump_log "mcs-dmlproc"
dump_log "mcs-loadbrm"
dump_log "mcs-primproc"
dump_log "mcs-workernode@1"
dump_log "mcs-writeengineserver"
