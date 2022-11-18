#!/usr/bin/env sh

set -x

COREDUMP=$2
BINARY=$1
FILENAME=$3
#SCRIPT_LOCATION=$(dirname "$0")
DUMPNAME=$4
STEP_NAME=$5

save_ansi_to_html ()
{
	echo "<b> $1 </b>" >> "${FILENAME}";
	cat "$DUMPNAME" | ansi2html --scheme=solarized >> "${FILENAME}"
}

invoke_gdb_command ()
{
	echo "gdb -q ${BINARY} --core ${COREDUMP} -ex '$1' -ex quit"
}


gdb -q "${BINARY}" --core ${COREDUMP} -ex 'bt' -ex quit >> "$DUMPNAME"
gdb -q "${BINARY}" --core ${COREDUMP} -ex 'info args' -ex quit >> "$DUMPNAME"
gdb -q "${BINARY}" --core ${COREDUMP} -ex 'info locals' -ex quit >> "$DUMPNAME"

echo "<b> Step: ${STEP_NAME}<br> Binary name: ${BINARY}<br> </b>" >> "${FILENAME}";
save_ansi_to_html "Backtrace"
save_ansi_to_html "Arguments"
save_ansi_to_html "Locals"
