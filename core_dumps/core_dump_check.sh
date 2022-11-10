#!/usr/bin/env sh

set -x

DIR_NAME=$1
RESULT=$2
STEP_NAME=$3
SCRIPT_LOCATION=$(dirname "$0")

invoke_format ()
{
    file=$1
    bash "$SCRIPT_LOCATION"/core_dump_format.sh "$(basename "${file%%_*}")" "$file" "$RESULT/$(basename "$file".html)" "$STEP_NAME"
}


for f in /"$DIR_NAME"/*; do
        case "$f" in
                *_core_dump.*)
                        invoke_format "$f"
                        ;;
        esac
done

for f in /"$DIR_NAME"/*; do
        case "$f" in
                *_core_dump.*)
                        echo "$(basename "${f%%_*}")" "aborted (core dumped)"
                        exit 1
                        ;;
        esac
done
