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
	script -q -c "${2}" | ansi2html --scheme=solarized >> "${FILENAME}"
}

invoke_gdb_command ()
{
	echo "gdb -q ${BINARY} --core ${COREDUMP} -ex '$1' -ex quit"
}


gdb -q ${BINARY} --core ${COREDUMP} -ex bt -ex quit > "$DUMPNAME"


echo "<b> Step: ${STEP_NAME}<br> Binary name: ${BINARY}<br> </b>" >> "${FILENAME}";
save_ansi_to_html "Backtrace" "$(invoke_gdb_command 'bt')"
save_ansi_to_html "Arguments" "$(invoke_gdb_command 'info args')"
save_ansi_to_html "Locals" "$(invoke_gdb_command 'info locals')"
