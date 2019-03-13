#!/bin/bash
#
# $Id$
#

usage="usage: run.sh [-vh] [-s sleep] [-t tries] [-l logDir] executable"

vflg=0
sopt="5"
topt="0"
lopt="/tmp/columnstore_tmp_files"

while getopts "vs:t:l:h" flag; do
	case $flag in
	v) vflg=1
		;;
	s) sopt=$OPTARG
		;;
	t) topt=$OPTARG
		;;
	l) lopt=$OPTARG
		;;
	h) echo $usage
		exit 0
		;;
	\? | *) echo $usage 1>&2
		exit 1
		;;
	esac
done

shift $((OPTIND-1))

exename="$@"

if [ -z "$exename" ]; then
	echo $usage 1>&2
	exit 1
fi

shift
args="$@"

retries=1
keep_going=1

if [ $vflg -gt 0 ]; then
	echo "starting $exename $args with sleep=$sopt and tries=$topt"
fi

while [ $keep_going -ne 0 ]; do
	$exename $args
	if [ -e ${lopt}/StopColumnstore ]; then
		exit 0
	fi
	if [ $topt -gt 0 -a $retries -ge $topt ]; then
		keep_going=0
	fi
	((retries++))
	if [ $keep_going -ne 0 -a $sopt -gt 0 ]; then
		sleep $sopt
	fi
done

if [ $vflg -gt 0 ]; then
	echo "$exename exceeded its try count, quiting!" 1>&2
fi

exit 1

