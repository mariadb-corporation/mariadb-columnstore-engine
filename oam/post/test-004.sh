#!/bin/bash
#
# $Id: test-004.sh 1538 2009-07-22 18:57:04Z dhill $

#
# Validates that FilesPerColumnPartition setting is not set lower than existing extents.
#

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
		
	. .$profileFile
fi

# Source function library.
if [ -f /etc/init.d/functions ]; then
	. /etc/init.d/functions
fi

if [ -z "$COLUMNSTORE_INSTALL_DIR" ]; then
	COLUMNSTORE_INSTALL_DIR=/usr/local/mariadb/columnstore
fi

export COLUMNSTORE_INSTALL_DIR=$COLUMNSTORE_INSTALL_DIR
export COLUMNSTORE_INSTALL_DIR=$COLUMNSTORE_INSTALL_DIR

test -f $COLUMNSTORE_INSTALL_DIR/post/functions && . $COLUMNSTORE_INSTALL_DIR/post/functions

scrname=`basename $0`
tname="validate-partition-size"

#Don't run on first boot
if firstboot; then
        exit 0
fi

exit 0

cplogger -i 48 $scrname "$tname"

# Get the FilesPerColumnPartition setting from Columnstore.xml.
filesPer=$(getConfig ExtentMap FilesPerColumnPartition)

# Get the maximum segment number for all column files.
maxSeg=$(editem -i | awk -F '|' -v max=0 '{if($7>max)max=$7}END{print max+1}')

# Error and out if the maximum existing segment number is higher than the FilesPerColumnPartition setting.
if [ $maxSeg -gt $filesPer ]; then
        cplogger -c 50 $scrname "$tname" "One or more tables were populated with FilesPerColumnPartition higher than the current setting."
        exit 1
fi

cplogger -i 52 $scrname "$tname"

exit 0

