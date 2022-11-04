#!/usr/bin/env sh

EXEC_NAME=$1

for f in /tmp/*; do
        case $f in
                *_core_dump.*)
                        bash core_dump_format.sh $EXEC_NAME $f "$f.html"
                        ;;
        esac
done

for f in /tmp/*; do
        case $f in
                *_core_dump.*)
                        exit 1
                        ;;
        esac
done
