#!/bin/bash
#
# $Id: quick_installer_single_server.sh 3705 2018-07-07 19:47:20Z dhill $
#
# Poddst- Quick Installer for Single Server MariaDB Columnstore

for arg in "$@"; do
	if [ `expr -- "$arg" : '--help'` -eq 6 ]; then
		echo "Usage ./quick_installer_single_server.sh"
		echo ""
		echo "Quick Installer for a Single Server MariaDB ColumnStore Install"
		echo ""
		exit 1
	else
		echo "quick_installer_single_server.sh: ignoring unknown argument: $arg" 1>&2
	fi
done


echo ""
echo "Run post-install script"
echo ""
columnstore-post-install
echo "Run postConfigure script"
echo ""
postConfigure -qs
