#!/bin/bash

section=$1
param=$2

if [ -f /usr/local/Calpont/bin/calpontConsole ]; then
	/usr/local/Calpont/bin/getConfig $section $param
else
	# If there's no calpontConsole, then we are running local with a single PM, probably Windows.
	# Note:  The "| awk" stuff below is a hack to make sygwin happy - it complains about invalid arithmetic operators
	#	 without it.
        x=`$INSTALLDIR/bin/getConfig $section $param | awk '{print $0}'`
        echo $x
fi

