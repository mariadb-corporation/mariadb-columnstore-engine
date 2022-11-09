#!/usr/bin/env sh

DIR_NAME=$1
RESULT=$2
SCRIPT_LOCATION=$(dirname "$0")


for f in /$DIR_NAME/*; do
        case $f in
                *_core_dump.*)
                        bash $SCRIPT_LOCATION/core_dump_format.sh $(basename ${f%%_*}) $f "$RESULT/$(basename $f.html)"
                        ;;
        esac
done
