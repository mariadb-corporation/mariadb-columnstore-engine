#!/bin/bash
#
# $Id: test-003.sh 2937 2012-05-30 18:17:09Z rdempsey $

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
tname="check-oid-bitmap"

#Don't run on first boot
if firstboot; then
	exit 0
fi

#Make sure there is an oid bitmap file if there are any EM entries

cplogger -i 48 $scrname "$tname"

obmfile=$(getConfig OIDManager OIDBitmapFile)
emcnt=$(editem -o 2001 | wc -l)

rc=1
if [ -f $obmfile -o $emcnt -eq 0 ]; then
	rc=0
fi

if [ $rc -ne 0 ]; then
	cplogger -c 50 $scrname "$tname" "there is no OID bitmap file but there are Extent Map entires"
	exit 1
fi

cplogger -i 52 $scrname "$tname"

exit 0

