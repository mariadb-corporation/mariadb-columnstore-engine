#!/usr/bin/env sh

set -x

SCRIPT_LOCATION=$(dirname "$0")

journalctl -u mcs-ddlproc.service > "$SCRIPT_LOCATION"/ddlproc.log
journalctl -u mcs-dmlproc.service > "$SCRIPT_LOCATION"/dmlproc.log
journalctl -u mcs-loadbrm.service > "$SCRIPT_LOCATION"/loadbrm.log
journalctl -u mcs-primproc.service > "$SCRIPT_LOCATION"/primproc.log
journalctl -u mcs-workernode@1.service > "$SCRIPT_LOCATION"/workernode.log
journalctl -u mcs-writeengineserver.service > "$SCRIPT_LOCATION"/writeengineserver.log
