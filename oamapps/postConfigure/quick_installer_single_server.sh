#!/bin/bash
#
# $Id: quick_installer_single_server.sh 3705 2018-07-07 19:47:20Z dhill $
#
# Poddst- Quick Installer for Single Server MariaDB Columnstore

for arg in "$@"; do
	if [ `expr -- "$arg" : '--help'` -eq 6 ]; then
		echo "Usage ./quick_installer_multi_server.sh"
		echo ""
		echo "Quick Installer for a Single Server MariaDB ColumnStore Install"
		echo ""
		exit 1
	else
		echo "quick_installer_multi_server.sh: ignoring unknown argument: $arg" 1>&2
	fi
done


if [ $HOME == "/root" ]; then
        echo "Run post-install script"
        echo ""
        /usr/local/mariadb/columnstore/bin/post-install
        echo "Run postConfigure script"
        echo ""
        /usr/local/mariadb/columnstore/bin/postConfigure -qs
else
        echo "Run post-install script"
        echo ""
        $HOME/mariadb/columnstore/bin/post-install --installdir=$HOME/mariadb/columnstore
        echo "Run postConfigure script"
        echo ""
        $HOME/mariadb/columnstore/bin/postConfigure -i $HOME/mariadb/columnstore -qs
fi
