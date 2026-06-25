#!/usr/bin/env sh

set -x
set -o pipefail


SCRIPT_LOCATION=$(dirname "$0")
LOG_PATH=$1

# save_stack runs ~80 min into a (likely hung) test and writes daemon stack traces via
# `tee $LOG_PATH/...`. tee does not create the directory, and on a hung test the test's own
# regression_logs.sh has not run yet (it only runs after go.sh exits), so $LOG_PATH may not
# exist and the traces would be silently lost. Ensure it exists first.
[ -n "$LOG_PATH" ] && mkdir -p "$LOG_PATH"


dump_stack ()
{
    name=$1
    echo "\nStack trace of $1"
    eu-stack -p `pidof $name` -n 0 | tee ${LOG_PATH}/${name}_callstacks.txt
}

dump_stack "mariadbd"
dump_stack "workernode"
dump_stack "controllernode"
dump_stack "WriteEngineServer"
dump_stack "DDLProc"
dump_stack "DMLProc"
dump_stack "PrimProc"
