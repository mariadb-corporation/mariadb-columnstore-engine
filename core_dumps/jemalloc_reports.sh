#!/usr/bin/env sh

set -x


DIR_NAME=$1
STEP_NAME=$2
SCRIPT_LOCATION=$(dirname "$0")

JEPROF_SOURCE="https://raw.githubusercontent.com/jemalloc/jemalloc/dev/bin/jeprof.in"

wget -qO- $JEPROF_SOURCE | sed "s/@jemalloc_version@/5.3.0/; s/@JEMALLOC_PREFIX@/ /" > jeprof
chmod +x jeprof

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
    ./jeprof --svg "$fullbin" "$file" > "/$DIR_NAME/${STEP_NAME}_$(basename $file).svg"
}


for f in /"$DIR_NAME"/*; do
        case "$f" in
                *.heap)
                        make_pdf "$f"
                        ;;
        esac
done
