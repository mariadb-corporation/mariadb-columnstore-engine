#!/bin/sh

db=$1

hostname=`hostname`

if [ "$hostname" == "srvalpha4.calpont.com" ]; then
	if [ "$db" == "ssb100c" ]; then
		db=ssb_100
	elif [ "$db" == "tpch1tc" ]; then
		db=tpch1t
	fi
fi

echo $db
