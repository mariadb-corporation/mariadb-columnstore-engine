#!/bin/bash
#
# $Id: quick_installer_single_server.sh 3705 2018-07-07 19:47:20Z dhill $
#
# Poddst- Quick Installer for Single Server MariaDB Columnstore


if [ $HOME == "root" ]; then
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
        $HOME/mariadb/columnstore/bin/postConfigure -i $HOME/mariadb/columnstore -sq
fi
