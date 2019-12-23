#!/bin/bash
#
# $Id: quick_installer_multi_server.sh 3705 2018-07-07 19:47:20Z dhill $
#
# Poddst- Quick Installer for Multi Server MariaDB Columnstore

pmIpAddrs=""
umIpAddrs=""
systemName=""

for arg in "$@"; do
	if [ `expr -- "$arg" : '--pm-ip-addresses='` -eq 18 ]; then
		pmIpAddrs="`echo $arg | awk -F= '{print $2}'`"
	elif [ `expr -- "$arg" : '--um-ip-addresses='` -eq 18 ]; then
		umIpAddrs="`echo $arg | awk -F= '{print $2}'`"
	elif [ `expr -- "$arg" : '--system-name='` -eq 14 ]; then
		systemName="`echo $arg | awk -F= '{print $2}'`"
		systemName="-sn "$systemName
	elif [ `expr -- "$arg" : '--help'` -eq 6 ]; then
		echo "Usage ./quick_installer_multi_server.sh [OPTION]"
		echo ""
		echo "Quick Installer for a Multi Server MariaDB ColumnStore Install"
		echo ""
		echo "Defaults to non-distrubuted install, meaning MariaDB Columnstore"
		echo "needs to be preinstalled on all nodes in the system"
		echo ""
		echo "Performace Module (pm) IP addresses are required"
		echo "User Module (um) IP addresses are option"
		echo "When only pm IP addresses provided, system is combined setup"
		echo "When both pm/um IP addresses provided, system is separate setup"
		echo
		echo "--pm-ip-addresses=xxx.xxx.xxx.xxx,xxx.xxx.xxx.xxx"
		echo "--um-ip-addresses=xxx.xxx.xxx.xxx,xxx.xxx.xxx.xxx, optional"
		echo "--system-name=nnnn System Name, optional"
		echo ""
		exit 1
	else
		echo "quick_installer_multi_server.sh: unknown argument: $arg, enter --help for help" 1>&2
		exit 1
	fi
done

if [[ $pmIpAddrs = "" ]]; then
	echo ""
	echo "Performace Module (pm) IP addresses required, exiting"
	exit 1
else
	if [[ $umIpAddrs = "" ]]; then
		echo ""
		echo "NOTE: Performing a Multi-Server Combined install with um/pm running on some server"
		echo""
	else
		echo ""
		echo "NOTE: Performing a Multi-Server separate install with um and pm running on separate servers"
		echo""
	fi
fi

echo ""
echo "${bold}Run post-install script${normal}"
echo ""
columnstore-post-install
echo "${bold}Run postConfigure script${normal}"
echo ""
if [[ $umIpAddrs = "" ]]; then
    postConfigure -qm -pm-ip-addrs $pmIpAddrs $systemName
else
    postConfigure -qm -pm-ip-addrs $pmIpAddrs -um-ip-addrs $umIpAddrs $systemName
fi
