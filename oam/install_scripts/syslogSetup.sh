#!/bin/bash
#
# $Id: syslogSetup.sh 421 2007-04-05 15:46:55Z dhill $
#
# syslogSetup.sh - install / uninstall MariaDB Columnstore system logging configuration

# no point in going any further if not root... (only works in bash)
#test $EUID -eq 0 || exit 0

prefix=/usr/local
installdir=$prefix/mariadb/columnstore
syslog_conf=nofile
rsyslog7=0

user=`whoami 2>/dev/null`

groupname=adm
username=syslog

for arg in "$@"; do
	if [ `expr -- "$arg" : '--prefix='` -eq 9 ]; then
		prefix="`echo $arg | awk -F= '{print $2}'`"
		installdir=$prefix/mariadb/columnstore
	elif [ `expr -- "$arg" : '--installdir='` -eq 13 ]; then
		installdir="`echo $arg | awk -F= '{print $2}'`"
		prefix=`dirname $installdir`
	elif [ `expr -- "$arg" : '--user='` -eq 7 ]; then
		user="`echo $arg | awk -F= '{print $2}'`"
		groupname=$user
		username=$user
	elif [ `expr -- "$arg" : '--..*'` -ge 3 ]; then
		echo "ignoring unknown argument: $arg" 1>&2
	elif [ `expr -- "$arg" : '--'` -eq 2 ]; then
		shift
		break
	else
		break
	fi
	shift
done

if [ $installdir != "/usr/local/mariadb/columnstore" ]; then
	export COLUMNSTORE_INSTALL_DIR=$installdir
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$COLUMNSTORE_INSTALL_DIR/lib
fi

columnstoreSyslogFile=$installdir/bin/columnstoreSyslog
columnstoreSyslogFile7=$installdir/bin/columnstoreSyslog7

checkSyslog() {
#check which syslog daemon is being used
#first check which is running 
daemon="nodaemon"
cnt=`ps -ef | grep -v grep | grep syslog-ng | wc -l`
if [ $cnt -gt 0 ]; then
	daemon="syslog-ng"
else cnt=`ps -ef | grep -v grep | grep rsyslog | wc -l`
	if [ $cnt -ge 1 ]; then
		daemon="rsyslog"
	else cnt=`ps -ef | grep -v grep | grep syslogd | wc -l`
		if [ $cnt -ge 1 ]; then
			if [ "$daemon" != "rsyslog" ]; then
				daemon=syslog
			fi
		fi
	fi
fi

#if none running, check installed
if [ "$daemon" = "nodaemon" ]; then

	if [ -f /etc/syslog.conf ]; then
		daemon="syslog"
		 /etc/init.d/syslog start > /dev/null 2>&1
	elif [ -f /etc/rsyslog.conf ]; then
		daemon="rsyslog"
		 /etc/init.d/rsyslog start > /dev/null 2>&1
	elif [ -f /etc/init.d/syslog-ng ]; then
		daemon="syslog-ng"
		 /etc/init.d/syslog-ng start > /dev/null 2>&1
	fi
fi

#if none running or installed, exit
if [ "$daemon" = "nodaemon" ]; then
	echo ""
	echo "*** No System Logging Application found (syslog, rsyslog, or syslog-ng)"
	echo "*** For MariaDB Columnstore System Logging functionality, install a System Logging package and reinstall MariaDB Columnstore"
	echo ""
	exit 1
fi

#check which syslog config file is installed
if [ "$daemon" = "syslog-ng" ]; then
	if [ -f /etc/syslog-ng/syslog-ng.conf ]; then
		syslog_conf=/etc/syslog-ng/syslog-ng.conf
		columnstoreSyslogFile=$installdir/bin/columnstoreSyslog-ng
		echo ""
		echo "System logging being used: syslog-ng"
		echo ""
	fi
elif [ "$daemon" = "rsyslog" ]; then
	#check if rsyslog version 7 or greater
	 rsyslogd -v > /tmp/rsyslog.ver
	cnt=`grep "rsyslogd 7" /tmp/rsyslog.ver | wc -l`
	if [ $cnt -gt 0 ]; then
		rsyslog7=1
	fi
	cnt=`grep "rsyslogd 8" /tmp/rsyslog.ver | wc -l`
	if [ $cnt -gt 0 ]; then
		rsyslog7=1
	fi
	cnt=`grep "rsyslogd 9" /tmp/rsyslog.ver | wc -l`
	if [ $cnt -gt 0 ]; then
		rsyslog7=1
	fi

	if [ -f /etc/rsyslog.conf ]; then
		cnt=`grep "/etc/rsyslog.d/" /etc/rsyslog.conf | wc -l`
		if [ $cnt -gt 0 ]; then
			if [ $rsyslog7 == 1 ]; then
				syslog_conf=/etc/rsyslog.d/49-columnstore.conf
			else
				syslog_conf=/etc/rsyslog.d/columnstore.conf
			fi
		else
			syslog_conf=/etc/rsyslog.conf
		fi
		echo ""
		echo "System logging being used: rsyslog"
		echo ""
	fi
elif [ "$daemon" = "syslog" ]; then
	if [ -f /etc/syslog.conf ]; then
		syslog_conf=/etc/syslog.conf
		echo ""
		echo "System logging being used: syslog"
		echo ""
	elif [ -d /etc/syslog-ng/syslog-ng.conf ]; then
		syslog_conf=/etc/syslog-ng/syslog-ng.conf
		columnstoreSyslogFile=$installdir/bin/columnstoreSyslog-ng
		echo ""
		echo "System logging being used: syslog-ng"
		echo ""
	fi
else
	echo ""
	echo "*** No System Logging Application found (syslog, rsyslog, or syslog-ng)"
	echo "*** For MariaDB Columnstore System Logging functionality, install a System Logging package and reinstall MariaDB Columnstore"
	echo ""
	exit 1
fi

}

install() {
checkSyslog
if [ ! -z "$syslog_conf" ] ; then
	$installdir/bin/setConfig -d Installation SystemLogConfigFile ${syslog_conf} >/dev/null 2>&1
	if [ $user != "root" ]; then
		 chown $user:$user /home/$user/mariadb/columnstore/etc/*
	fi 
	
	if [ "$syslog_conf" != /etc/rsyslog.d/columnstore.conf ]; then
		 rm -f ${syslog_conf}.columnstoreSave
		 cp ${syslog_conf} ${syslog_conf}.columnstoreSave >/dev/null 2>&1
		 sed -i '/# MariaDB/,$d' ${syslog_conf}.columnstoreSave > /dev/null 2>&1
	fi

	egrep -qs 'MariaDB Columnstore Database Platform Logging' ${syslog_conf}
	if [ $? -ne 0 ]; then
		#set the syslog for ColumnStore logging
		# remove older version incase it was installed by previous build
		 rm -rf /etc/rsyslog.d/columnstore.conf

		// determine username/groupname
		
		if [ -f /var/log/messages ]; then
		      user=`stat -c "%U %G" /var/log/messages | awk '{print $1}'`
		      group=`stat -c "%U %G" /var/log/messages | awk '{print $2}'`
		fi
		
		if [ -f /var/log/syslog ]; then
		      user=`stat -c "%U %G" /var/log/syslog | awk '{print $1}'`
		      group=`stat -c "%U %G" /var/log/syslog | awk '{print $2}'`
		fi
		
		//set permissions
		$SUDO chown $user:$group -R /var/log/mariadb > /dev/null 2>&1
		
		if [ $rsyslog7 == 1 ]; then
			 rm -f /etc/rsyslog.d/49-columnstore.conf
			 cp  ${columnstoreSyslogFile7} ${syslog_conf}

			 sed -i -e s/groupname/$groupname/g ${syslog_conf}
			 sed -i -e s/username/$username/g ${syslog_conf}
		else		
			 cp  ${columnstoreSyslogFile} ${syslog_conf}
		fi
	fi

	# install Columnstore Log Rotate File
	 cp $installdir/bin/columnstoreLogRotate /etc/logrotate.d/columnstore > /dev/null 2>&1
	 chmod 644 /etc/logrotate.d/columnstore

	restartSyslog
fi

}

uninstall() {
checkSyslog
if [ ! -z "$syslog_conf" ] ; then
	if [ "$syslog_conf" != /etc/rsyslog.d/columnstore.conf ]; then
		if [ "$syslog_conf" != /etc/rsyslog.d/49-columnstore.conf ]; then
			egrep -qs 'MariaDB Columnstore Database Platform Logging' ${syslog_conf}
			if [ $? -eq 0 ]; then
				if [ -f ${syslog_conf}.columnstoreSave ] ; then
					#uninstall the syslog for ColumnStore logging
					 v -f ${syslog_conf} ${syslog_conf}.ColumnStoreBackup
					 mv -f ${syslog_conf}.columnstoreSave ${syslog_conf} >/dev/null 2>&1
					if [ ! -f ${syslog_conf} ] ; then
						 cp ${syslog_conf}.ColumnStoreBackup ${syslog_conf}
					fi
				fi
			fi
			 sed -i '/# MariaDB/,$d' ${syslog_conf} > /dev/null 2>&1
		else
			 rm -f "$syslog_conf"
		fi
	else
		 rm -f "$syslog_conf"
	fi
	
	# uninstall Columnstore Log Rotate File
	rm -f /etc/logrotate.d/columnstore

	restartSyslog

fi

}

status() {
checkSyslog
if [ ! -z "$syslog_conf" ] ; then
	egrep -qs 'MariaDB Columnstore Database Platform Logging' ${syslog_conf}
	if [ $? -eq 0 ]; then
		echo $syslog_conf
	else
		echo "No System Log Config File configured for MariaDB Columnstore System Logging"
	fi
fi
}

check() {
test -f $installdir/post/functions && . $installdir/post/functions
number=$RANDOM
$installdir/bin/cplogger -i 104 "MariaDB Columnstore Log Test: $number"
sleep 3
egrep -qs "MariaDB Columnstore Log Test: $number" /var/log/mariadb/columnstore/info.log
if [ $? -eq 0 ]; then
	echo "MariaDB Columnstore System Logging working"
	exit 0
else
	echo "MariaDB Columnstore System Logging not working"
	exit 1
fi
}

restartSyslog() {

	if [ "$daemon" = "syslog-ng" ]; then
		if [ -f /etc/init.d/syslog-ng ]; then
			 /etc/init.d/syslog-ng restart  > /dev/null 2>&1
		else
		         systemctl restart syslog-ng.service > /dev/null 2>&1
		fi
	elif [ "$daemon" = "rsyslog" ]; then
                if [ -f /etc/init.d/rsyslog ]; then
                         /etc/init.d/rsyslog restart  > /dev/null 2>&1
                else
                         systemctl restart rsyslog.service > /dev/null 2>&1
                fi
	elif [ "$daemon" = "syslog" ]; then	
                if [ -f /etc/init.d/syslog ]; then
                         /etc/init.d/syslog restart  > /dev/null 2>&1
                else
                         systemctl restart syslog.service > /dev/null 2>&1
                fi
	fi
}

case "$1" in
  install)
  	install
	;;
  uninstall)
  	uninstall
	;;
  status)
  	status
	;;
  check)
  	check
	;;
  *)
	echo $"Usage: $0 {install|uninstall|status|check)"
	exit 1
esac


exit 0
/
