#!/bin/bash
#
# $Id: test-001.sh 3704 2013-08-07 03:33:20Z bwilkinson $

prefix=/usr/local

USER=`whoami 2>/dev/null`

if [ $USER != "root" ]; then
	prefix=$HOME
fi

if [ $USER != "root" ]; then
	if [ -f $prefix/.bash_profile ]; then		
		profileFile=$prefix/.bash_profile
	elif [ -f $prefix/.profile ]; then		
		profileFile=$prefix/.profile
	else
		profileFile=$prefix/.bashrc
	fi
		
	. $profileFile
fi

# Source function library.
if [ -f /etc/init.d/functions ]; then
	. /etc/init.d/functions
fi

if [ -z "$COLUMNSTORE_INSTALL_DIR" ]; then
	COLUMNSTORE_INSTALL_DIR=/usr/local/mariadb/columnstore
fi

export COLUMNSTORE_INSTALL_DIR=$COLUMNSTORE_INSTALL_DIR

test -f $COLUMNSTORE_INSTALL_DIR/post/functions && . $COLUMNSTORE_INSTALL_DIR/post/functions

scrname=`basename $0`
tname="check-syscat-oids"

mt=`module_type`

#These tests only for PM
if [ "$mt" != "pm" ]; then
	exit 0
fi

#check for dbrm and data1, don't run if missing both
if firstboot; then
	if [ -d $COLUMNSTORE_INSTALL_DIR/data1/000.dir ]; then
		cplogger -c 50 $scrname "$tname" "missing dbrm data with existing 000.dir"
		exit 1
	else
		exit 0
	fi
else
	#check for oidbitmap file
	if oidbitmapfile; then
		cplogger -c 50 $scrname "$tname" "missing oidbitmapfile with existing current file"
		exit 1
	fi
fi

#check for both current file and OIDBITMAP file

#Make sure all syscat OIDs are present (N.B. only works for shared-everything)

cplogger -i 48 $scrname "$tname"

catoids=
catoids="$catoids 1001 1002 1003 1004 1005 1006 1007 1008 1009 1010"
catoids="$catoids 2001 2004"
catoids="$catoids 1021 1022 1023 1024 1025 1026 1027 1028 1029 1030 1031 1032 1033 1034 1035 1036 1037 1038 1039 1040"
catoids="$catoids 2061 2064 2067 2070 2073 2076"

# TODO-this doesn't work with HDFS file system
#for oid in $catoids; do
#	if [ ! -s `oid2file $oid` ]; then
#		cplogger -c 50 $scrname "$tname" "could not find file for OID $oid"
#		exit 1
#	fi
#done

cplogger -i 52 $scrname "$tname"

exit 0

