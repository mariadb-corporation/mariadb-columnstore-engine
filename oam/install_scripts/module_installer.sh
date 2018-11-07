#!/bin/bash
#
# $Id: module_installer.sh 421 2007-04-05 15:46:55Z dhill $
#
# Setup the Custom OS files during a System install on a module
#
#
# append columnstore OS files to Linux OS file
#
#

prefix=/usr/local
installdir=$prefix/mariadb/columnstore
rpmmode=install
user=`whoami 2>/dev/null`
quiet=0
shiftcnt=0
password=" "

for arg in "$@"; do
	if [ $(expr -- "$arg" : '--prefix=') -eq 9 ]; then
		prefix="$(echo $arg | awk -F= '{print $2}')"
		installdir=$prefix/mariadb/columnstore
		((shiftcnt++))
	elif [ $(expr -- "$arg" : '--rpmmode=') -eq 10 ]; then
		rpmmode="$(echo $arg | awk -F= '{print $2}')"
		((shiftcnt++))
	elif [ $(expr -- "$arg" : '--installdir=') -eq 13 ]; then
		installdir="$(echo $arg | awk -F= '{print $2}')"
		prefix=$(dirname $installdir)
		((shiftcnt++))
	elif [ $(expr -- "$arg" : '--user=') -eq 7 ]; then
		user="$(echo $arg | awk -F= '{print $2}')"
		((shiftcnt++))
	elif [ $(expr -- "$arg" : '--quiet') -eq 7 ]; then
		quiet=1
		((shiftcnt++))
	elif [ $(expr -- "$arg" : '--port') -eq 6 ]; then
		mysqlPort="$(echo $arg | awk -F= '{print $2}')"
		((shiftcnt++))
	elif [ $(expr -- "$arg" : '--module') -eq 8 ]; then
		module="$(echo $arg | awk -F= '{print $2}')"
		((shiftcnt++))
	elif [ $(expr -- "$arg" : '--password') -eq 10 ]; then
		password="$(echo $arg | awk -F= '{print $2}')"
		((shiftcnt++))
	fi
done
shift $shiftcnt

if [ $installdir != "/usr/local/mariadb/columnstore" ]; then
	export COLUMNSTORE_INSTALL_DIR=$installdir
	export PATH=$COLUMNSTORE_INSTALL_DIR/bin:$COLUMNSTORE_INSTALL_DIR/mysql/bin:/bin:/usr/bin
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$COLUMNSTORE_INSTALL_DIR/lib:$COLUMNSTORE_INSTALL_DIR/mysql/lib
else
	export COLUMNSTORE_INSTALL_DIR=$installdir
fi

PMwithUM=`$COLUMNSTORE_INSTALL_DIR/bin/getConfig Installation PMwithUM`
ServerTypeInstall=`$COLUMNSTORE_INSTALL_DIR/bin/getConfig Installation ServerTypeInstall`

#get temp directory
tmpDir=`$installdir/bin/getConfig SystemConfig SystemTempFileDir`

cloud=`$COLUMNSTORE_INSTALL_DIR/bin/getConfig Installation Cloud`
if [ $cloud = "amazon-ec2" ] || [ $cloud = "amazon-vpc" ]; then
	echo "Amazon setup on Module"
	cp $COLUMNSTORE_INSTALL_DIR/local/etc/credentials $HOME/.aws/. > /dev/null 2>&1
	
	if [ $module = "pm" ]; then
		if test -f $COLUMNSTORE_INSTALL_DIR/local/etc/pm1/fstab ; then
			echo "Setup fstab on Module"
			SUDO=""
			if [ $user != "root" ]; then
				SUDO="sudo "
			fi
			touch /etc/fstab
			$SUDO chmod 666 /etc/fstab
			rm -f /etc/fstab.columnstoreSave
			cp /etc/fstab /etc/fstab.columnstoreSave
			cat $COLUMNSTORE_INSTALL_DIR/local/etc/pm1/fstab >> /etc/fstab
		fi
	fi
fi

test -f $COLUMNSTORE_INSTALL_DIR/post/functions && . $COLUMNSTORE_INSTALL_DIR/post/functions

mid=`module_id`

#if um, cloud, separate system type, external um storage, then setup mount
if [ $cloud = "amazon-ec2" ] || [ $cloud = "amazon-vpc" ]; then
	if [ $module = "um" ]; then
		systemtype=`$COLUMNSTORE_INSTALL_DIR/bin/getConfig Installation ServerTypeInstall`
		if [ $systemtype = "1" ]; then
			umstoragetype=`$COLUMNSTORE_INSTALL_DIR/bin/getConfig Installation UMStorageType`
			if [ $umstoragetype = "external" ]; then
				echo "Setup UM Volume Mount"
				device=`$COLUMNSTORE_INSTALL_DIR/bin/getConfig Installation UMVolumeDeviceName$mid`
				mkdir -p $COLUMNSTORE_INSTALL_DIR/mysql/db > /dev/null 2>&1
				sudo mount $device $COLUMNSTORE_INSTALL_DIR/mysql/db -t ext2 -o noatime,nodiratime,noauto,user
				sudo chown $user:$user -R $COLUMNSTORE_INSTALL_DIR/mysql > /dev/null 2>&1
			fi
		fi
	fi

	#setup rc.local for amazon
	RCFILE=/etc/rc.d/rc.local

	if [ $user != "root" ]; then
		  echo "uncomment runuser in rc.local, amazon AMI"
		  sudo sed -i -e 's/#runuser/runuser/g' /etc/rc.d/rc.local >/dev/null 2>&1
	fi
fi

#if pm, create dbroot directories
if [ $module = "pm" ]; then
	numdbroots=`$COLUMNSTORE_INSTALL_DIR/bin/getConfig SystemConfig DBRootCount`
	for (( id=1; id<$numdbroots+1; id++ )); do
		mkdir -p $COLUMNSTORE_INSTALL_DIR/data$id > /dev/null 2>&1
		chmod 755 $COLUMNSTORE_INSTALL_DIR/data$id
	done
fi

plugin=`$COLUMNSTORE_INSTALL_DIR/bin/getConfig SystemConfig DataFilePlugin`
if [ -n "$plugin" ]; then
	echo "Setup .bashrc on Module for local-query"

	setenv=`$COLUMNSTORE_INSTALL_DIR/bin/getConfig SystemConfig DataFileEnvFile`

	eval userhome=~$user
	bashFile=$userhome/.bashrc
	touch ${bashFile}

	echo " " >> ${bashFile}
	echo ". $COLUMNSTORE_INSTALL_DIR/bin/$setenv" >> ${bashFile}
fi

# if mysqlrep is on and module has a my.cnf file, upgrade it

MySQLRep=`$COLUMNSTORE_INSTALL_DIR/bin/getConfig Installation MySQLRep`
if [ $MySQLRep = "y" ]; then
	if test -f $COLUMNSTORE_INSTALL_DIR/mysql/my.cnf ; then
		echo "Run Upgrade on my.cnf on Module"
		$COLUMNSTORE_INSTALL_DIR/bin/mycnfUpgrade > ${tmpDir}/mycnfUpgrade.log 2>&1
	fi
fi

if test -f $COLUMNSTORE_INSTALL_DIR/mysql/my.cnf ; then
	mysqlPort=`$COLUMNSTORE_INSTALL_DIR/bin/getConfig Installation MySQLPort`
	echo "Run Mysql Port update on my.cnf on Module"
	$COLUMNSTORE_INSTALL_DIR/bin/mycnfUpgrade $mysqlPort > ${tmpDir}/mycnfUpgrade_port.log 2>&1
fi

# if um, run mysql install scripts
if [ $module = "um" ] || ( [ $module = "pm" ] && [ $PMwithUM = "y" ] ) || [ $ServerTypeInstall = "2" ]; then

	mysqlPassword=" "
	if [[ $password != " " ]]; then
		mysqlPassword="--password="$password
	fi

	echo "Run post-mysqld-install"
	$COLUMNSTORE_INSTALL_DIR/bin/post-mysqld-install --installdir=$COLUMNSTORE_INSTALL_DIR $mysqlPassword --tmpdir=${tmpDir} > ${tmpDir}/post-mysqld-install.log 2>&1
	if [ $? -ne 0 ]; then
	    echo "ERROR: post-mysqld-install failed: check ${tmpDir}/post-mysqld-install.log"
	    exit 1
	fi
	
	echo "Run post-mysql-install"
	
	$COLUMNSTORE_INSTALL_DIR/bin/post-mysql-install --installdir=$COLUMNSTORE_INSTALL_DIR --tmpdir=${tmpDir} > ${tmpDir}/post-mysql-install.log 2>&1
        if [ $? -ne 0 ]; then
            echo "ERROR: post-mysql-install failed: check ${tmpDir}/post-mysql-install.log"
            exit 1
	fi
fi

if [ $user == "root" ]; then
    $COLUMNSTORE_INSTALL_DIR/bin/syslogSetup.sh check > ${tmpDir}/syslogSetup-check.log 2>&1
    if [ $? -ne 0 ]; then
	    # try setup again
	    $COLUMNSTORE_INSTALL_DIR/bin/syslogSetup.sh install > ${tmpDir}/syslogSetup-install.log 2>&1
	    if [ $? -ne 0 ]; then
		    echo "WARNING: syslogSetup.sh check failed: check ${tmpDir}/syslogSetup-check.log"
		    exit 2
	    fi
    fi
fi

echo "!!!Module Installation Successfully Completed!!!"

exit 0
