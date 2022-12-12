#!/usr/bin/env sh

set -x


DIR_NAME=$1
STEP_NAME=$2
SCRIPT_LOCATION=$(dirname "$0")

parse_heap_name ()
{
    name=$1
    name="$(basename $name)"
    name="${name%%_*}"
    echo "$(which $name)"
}


make_pdf ()
{
    file=$1
    fullbin="$(parse_heap_name "$file")"
    binname="$(basename "$fullbin")"
    jeprof --svg "$fullbin" "$file" > "/$DIR_NAME/${STEP_NAME}_$(basename $file).svg"
}


for f in /"$DIR_NAME"/*; do
        case "$f" in
                *.heap)
                        make_pdf "$f"
                        ;;
        esac
done
