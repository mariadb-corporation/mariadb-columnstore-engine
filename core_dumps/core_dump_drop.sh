#!/usr/bin/env sh

DIR_NAME=$1

for f in /"$DIR_NAME"/*; do
        case "$f" in
                *_core_dump.*)
                        echo "$(basename "${f%%_*}")" "aborted (core dumped)"
                        exit 1
                        ;;
        esac
done
