#!/bin/bash

# Top level script for automatically restart failed columnstore processes.
# Use it to start any columnstore process.
# Eg:
#     loop_process_starter.sh workernode DBRM_Worker2
#     loop_process_starter.sh PrimProc

PROGNAME="$0"
USAGE="Usage: ${PROGNAME} [binary_name:required] [binary_startup_args|optional] [log_filename|optional]"
BINARY_NAME="${1:?No binary name given. ${USAGE}}"
ARG=${2:-""}
LOWER_BINARY_NAME="$(echo ${BINARY_NAME} | tr [:upper:] [:lower:])"
LOG_FILENAME="${3:-${LOWER_BINARY_NAME}}.log"

while true; do
    echo $BINARY_NAME $ARG $LOG_FILENAME
    ${BINARY_NAME} ${ARG} &>> ${LOG_FILENAME}
    echo "$(date): ${BINARY_NAME} failed, restarting." >> ${LOG_FILENAME}
    sleep 1
done
