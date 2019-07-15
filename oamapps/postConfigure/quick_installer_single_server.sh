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


if [ $HOME == "/root" ]; then
        echo ""
        echo "Run post-install script"
        echo ""
        /usr/local/mariadb/columnstore/bin/post-install
        echo "Run postConfigure script"
        echo ""
        /usr/local/mariadb/columnstore/bin/postConfigure -qs
else
        echo ""
        echo "Run post-install script"
        echo ""
        $HOME/mariadb/columnstore/bin/post-install --installdir=$HOME/mariadb/columnstore
        
		export COLUMNSTORE_INSTALL_DIR=$HOME/mariadb/columnstore
		export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HOME/mariadb/columnstore/lib:$HOME/mariadb/columnstore/mysql/lib

        echo "Run postConfigure script"
        echo ""
        $HOME/mariadb/columnstore/bin/postConfigure -i $HOME/mariadb/columnstore -qs
fi
