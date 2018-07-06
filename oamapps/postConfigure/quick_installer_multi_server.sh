#!/bin/bash
#
# $Id: quick_installer_multi_server.sh 3705 2018-07-07 19:47:20Z dhill $
#
# Poddst- Quick Installer for Multi Server MariaDB Columnstore

pmIpAddrs=""
umIpAddrs=""

for arg in "$@"; do
	if [ `expr -- "$arg" : '--pm-ip-addresses='` -eq 18 ]; then
		pmIpAddrs="`echo $arg | awk -F= '{print $2}'`"
	elif [ `expr -- "$arg" : '--pm-ip-addresses='` -eq 18 ]; then
		umIpAddrs="`echo $arg | awk -F= '{print $2}'`"
	elif [ `expr -- "$arg" : '--help'` -eq 6 ]; then
		echo "Usage ./quick_installer_multi_server.sh [OPTION]"
		echo ""
		echo "Quick Installer for a Multi Server MariaDB ColumnStore Install"
		echo ""
		echo "Performace Module (pm) IP addresses required"
		echo "User Module (um) IP addresses option"
		echo "When only pm IP addresses provided, system is combined setup"
		echo "When both pm/um IP addresses provided, system is seperate setup"
		echo
		echo "--pm-ip-addresses=xxx.xxx.xxx.xxx,xxx.xxx.xxx.xxx"
		echo "--um-ip-addresses=xxx.xxx.xxx.xxx,xxx.xxx.xxx.xxx"
		echo ""
	else
		echo "quick_installer_multi_server.sh: ignoring unknown argument: $arg" 1>&2
	fi
done

if [ $pmIpAddrs == "" ] ; then
	echo ""
	echo "Performace Module (pm) IP addresses required, exiting"
	exit 1
else
	if [ $umIpAddrs == "" ] ; then
		echo ""
		echo "Performing a Multi-Server Combined install with um/pm running on some server"
		echo""
	else
		echo ""
		echo "Performing a Multi-Server Seperate install with um and pm running on seperate servers"
		echo""
	fi
fi

if [ $HOME == "/root" ]; then
        echo "Run post-install script"
        echo ""
        /usr/local/mariadb/columnstore/bin/post-install
        echo "Run postConfigure script"
        echo ""
        /usr/local/mariadb/columnstore/bin/postConfigure -qm -pm-ip-addrs=$pmIpAddrs -um-ip-addrs=$umIpAddrs
else
        echo "Run post-install script"
        echo ""
        $HOME/mariadb/columnstore/bin/post-install --installdir=$HOME/mariadb/columnstore
        echo "Run postConfigure script"
        echo ""
        $HOME/mariadb/columnstore/bin/postConfigure -i $HOME/mariadb/columnstore -qm -pm-ip-addrs=$pmIpAddrs -um-ip-addrs=$umIpAddrs
fi
