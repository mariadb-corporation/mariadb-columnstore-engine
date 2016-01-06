#!/bin/bash
#
# Restarts the database.
#

if [ -f /usr/local/Calpont/bin/calpontConsole ]; then
	/usr/local/Calpont/bin/calpontConsole restartsystem y Force
	/usr/local/Calpont/mysql/mysql-Calpont restart
else
	# If there's no calpontConsole, then we are running local with a single PM; better be Windows.
	idbsvsto.bat
	idbsvsta.bat
	sleep 3
	echo restarted
fi

