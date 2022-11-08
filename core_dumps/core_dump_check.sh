#!/usr/bin/env sh

EXEC_NAME=$1
RESULT=$2

for f in /core/*; do
        case $f in
                *_core_dump.*)
                        bash core_dump_format.sh $EXEC_NAME $f "$RESULT/$(basename $f.html)"
                        ;;
        esac
done
