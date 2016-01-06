#!/bin/bash

script_dir=$(dirname $0)
source $script_dir/oamCommands.sh

section=$1
param=$2
value=$3

if [ -f /usr/local/Calpont/bin/calpontConsole ]; then

	#
	# If this is a multi node stack, call setConfig on pm1.
	#
	if `isMultiNode`; then
		WRITENODEPMNUM=`/usr/local/Calpont/bin/calpontConsole getsystemstatus | grep Active | awk '{print substr($7, 2, 3)}'`
		if [ -z $WRITENODEPMNUM ]; then
			WRITENODEPMNUM='pm1'
		fi

		PASSWORD=`$script_dir/getPassWord.sh`
		WRITENODENAME=`/usr/local/Calpont/bin/calpontConsole getsystemnet | grep $WRITENODEPMNUM | grep Performance | awk '{print $6}'`
		/usr/local/Calpont/bin/remote_command.sh $WRITENODENAME $PASSWORD "/usr/local/Calpont/bin/setConfig $section $param $value"
	#
	# Else call setConfig locally.
	#
	else
		/usr/local/Calpont/bin/setConfig $section $param $value
	fi	
else
	# If there's no calpontConsole, then we are running local with a single PM, probably Windows.
	$INSTALLDIR/bin/setConfig $section $param $value
fi

