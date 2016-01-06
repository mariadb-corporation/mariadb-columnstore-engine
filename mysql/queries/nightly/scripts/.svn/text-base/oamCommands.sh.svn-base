#!/bin/bash

#
# Echos true if stack has multiple nodes, otherwise false.  Looks for a um1 to determine it's a multi node.
# TODO:  Will need to change if we start running on a multi node with UM/PM combos.
#
isMultiNode() {
	if $WINDOWS; then
		echo false
	else
		multiNode=`/usr/local/Calpont/bin/calpontConsole getsystemstatus | grep um1 | wc -l`
		if [ $multiNode -ge 1 ]; then
			echo true
		else
			echo false
		fi
	fi
}

#
# Restarts the database.
#
restartSystem() {
	if $WINDOWS; then
	        idbsvsto.bat
        	idbsvsta.bat
		# TODO:  Take out the sleep below assuming everything is working.
		sleep 120
	else
        	/usr/local/Calpont/bin/calpontConsole restartsystem y Force
	        /usr/local/Calpont/mysql/mysql-Calpont restart
	fi
}

#
# Echos the InfiniDB version.
#
getVersion() {
	if $WINDOWS; then
		grep Version $WINDRIVE/Calpont/etc/CalpontVersion.txt | awk '{print $3}'
	else
		/usr/local/Calpont/bin/calpontConsole getcalponts | grep Version | awk '{print $3}'
	fi
}

#
# Echos the InfiniDB release num.
#
getRelease() {
	if $WINDOWS; then
		grep Release $WINDRIVE/Calpont/etc/CalpontVersion.txt | awk '{print $3}'
	else
		/usr/local/Calpont/bin/calpontConsole getcalponts | grep Release | awk '{print $3}'
	fi
}

#
# Echos the build datetime.
#
getBuildDtm() {
	if $WINDOWS; then
		grep "Build Date" $WINDRIVE/Calpont/etc/CalpontVersion.txt | awk '{print $6 " " $7}'
	else
	    /usr/local/Calpont/bin/calpontConsole getcalponts | grep "Build Date" | awk '{print $6 " " $7 " " $8 " " $9 " " $10}'
	fi
}


