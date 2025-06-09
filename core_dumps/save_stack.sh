#!/usr/bin/env sh

set -x
set -o pipefail


SCRIPT_LOCATION=$(dirname "$0")
LOG_PATH=$1


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
