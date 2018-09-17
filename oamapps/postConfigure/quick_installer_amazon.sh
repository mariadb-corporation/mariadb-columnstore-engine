#!/bin/bash
#
# $Id: quick_installer_amazon.sh 3705 2018-07-07 19:47:20Z dhill $
#
# Poddst- Quick Installer for Amazon MariaDB Columnstore

pmCount=""
umCount=""
systemName=""

for arg in "$@"; do
	if [ `expr -- "$arg" : '--pm-count='` -eq 11 ]; then
		pmCount="`echo $arg | awk -F= '{print $2}'`"
	elif [ `expr -- "$arg" : '--um-count='` -eq 11 ]; then
		umCount="`echo $arg | awk -F= '{print $2}'`"
	elif [ `expr -- "$arg" : '--system-name='` -eq 14 ]; then
		systemName="`echo $arg | awk -F= '{print $2}'`"
		systemName="-sn "$systemName
	elif [ `expr -- "$arg" : '--dist-install'` -eq 14 ]; then
		nonDistrubutedInstall=" "
	elif [ `expr -- "$arg" : '--help'` -eq 6 ]; then
		echo "Usage ./quick_installer_amazon.sh [OPTION]"
		echo ""
		echo "Quick Installer for an Amazon MariaDB ColumnStore Install"
		echo "This requires to be run on a MariaDB ColumnStore AMI"
		echo ""
		echo "Performace Module (pm) number is required"
		echo "User Module (um) number is option"
		echo "When only pm counts provided, system is combined setup"
		echo "When both pm/um counts provided, system is seperate setup"
		echo
		echo "--pm-count=x Number of pm instances to create"
		echo "--um-count=x Number of um instances to create, optional"
		echo "--system-name=nnnn System Name, optional"
		echo ""
		exit 1
	else
		echo "./quick_installer_amazon.sh: unknown argument: $arg, enter --help for help" 1>&2
		exit 1
	fi
done

if [[ $pmCount = "" ]]; then
	echo ""
	echo "Performace Module (pm) count is required, exiting"
	exit 1
else
	if [[ $umCount = "" ]]; then
		echo ""
		echo "NOTE: Performing a Multi-Server Combined install with um/pm running on some server"
		echo""
	else
		echo ""
		echo "NOTE: Performing a Multi-Server Seperate install with um and pm running on seperate servers"
		echo""
	fi
fi

if [[ $HOME = "/root" ]]; then
        echo "${bold}Run post-install script${normal}"
        echo ""
        /usr/local/mariadb/columnstore/bin/post-install
        echo "${bold}Run postConfigure script${normal}"
        echo ""
        if [[ $umCount = "" ]]; then
			/usr/local/mariadb/columnstore/bin/postConfigure -qa -pm-count $pmCount $systemName
		else
			/usr/local/mariadb/columnstore/bin/postConfigure -qa -pm-count $pmCount -um-count $umCount $systemName
		fi
else
        echo "${bold}Run post-install script${normal}"
        echo ""
        $HOME/mariadb/columnstore/bin/post-install --installdir=$HOME/mariadb/columnstore
        echo "${bold}Run postConfigure script${normal}"
        echo ""
        if [[ $umCount = "" ]]; then
			. /etc/profile.d/columnstoreEnv.sh;$HOME/mariadb/columnstore/bin/postConfigure -i $HOME/mariadb/columnstore -qa -pm-count $pmCount $systemName
		else
			. /etc/profile.d/columnstoreEnv.sh;$HOME/mariadb/columnstore/bin/postConfigure -i $HOME/mariadb/columnstore -qa -pm-count $pmCount -um-count $umCount $systemName
		fi
fi
