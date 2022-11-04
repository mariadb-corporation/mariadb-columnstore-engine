#!/usr/bin/env sh

COREDUMP=$2
BINARY=$1
FILENAME=$3
SCRIPT_LOCATION=$(dirname "$0")

save_ansi_to_html ()
{
	echo "<b> $1 </b>" >> $FILENAME;
	script -q -c "${2}" | bash $SCRIPT_LOCATION/ansi2html.sh --palette=solarized >> $FILENAME
}

invoke_gdb_command ()
{
	echo "gdb -q $BINARY --core $COREDUMP -ex '$1' -ex quit"
}

save_ansi_to_html "Backtrace" "$(invoke_gdb_command 'bt')"
save_ansi_to_html "Arguments" "$(invoke_gdb_command 'info args')"
save_ansi_to_html "Locals" "$(invoke_gdb_command 'info locals')"
