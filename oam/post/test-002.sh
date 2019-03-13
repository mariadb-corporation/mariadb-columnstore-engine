#!/bin/bash
#
# $Id: test-002.sh 2937 2012-05-30 18:17:09Z rdempsey $

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
tname="check-brm"

#Don't run on first boot
if firstboot; then
	exit 0
fi

#Make sure BRM is read-write

cplogger -i 48 $scrname "$tname"

#turn this test off for now...it doesn't if the DBRM isn't started, and these tests run too early
# we need a way to run some tests at different stages of system startup...
#dbrmctl status 2>&1 | egrep -qsi '^ok'
/bin/true
rc=$?

if [ $rc -ne 0 ]; then
	cplogger -c 50 $scrname "$tname" "the BRM is read only"
	exit 1
fi

cplogger -i 52 $scrname "$tname"

exit 0

