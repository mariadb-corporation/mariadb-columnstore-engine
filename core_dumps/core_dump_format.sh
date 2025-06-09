#!/usr/bin/env sh

set -x

COREDUMP=$2
BINARY=$1
FILENAME=$3
SCRIPT_LOCATION=$(dirname "$0")
DUMPNAME=$4
STEP_NAME=$5

save_ansi_to_html() {

	echo "<h2> $1 </h2>" >>"${FILENAME}"
	cat "$DUMPNAME" | bash "${SCRIPT_LOCATION}"/ansi2html.sh --palette=solarized >>"${FILENAME}"
}
invoke_gdb_command() {
	unbuffer gdb -x "${SCRIPT_LOCATION}"/gdbinit -q ${BINARY} --core ${COREDUMP} -ex "$1" -ex quit >>"$DUMPNAME"
}

echo "<h1> Step: ${STEP_NAME}<br> Binary name: ${BINARY}<br> </h1>" >>"${FILENAME}"

invoke_gdb_command 'bt full'
save_ansi_to_html "Backtrace"

invoke_gdb_command "info args"
save_ansi_to_html "Arguments"

invoke_gdb_command "info locals"
save_ansi_to_html "Locals"

gzip -5 "${COREDUMP}"
