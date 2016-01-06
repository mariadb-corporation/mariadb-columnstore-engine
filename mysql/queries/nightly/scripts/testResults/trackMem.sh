#!/bin/bash
#
# Usage:
# trackMem.sh runId testName

runId=$1
test=$2

PrimProcMem=0
ExeMgrMem=0
DMLProcMem=0
importMem=0
DDLProcMem=0
controllernodeMem=0
workernodeMem=0

while [ true ]
do
	dt=`date '+%Y-%m-%d %H:%M:%S'`
        if ! $WINDOWS;then
		i=0
		for process in PrimProc ExeMgr DMLProc cpimport DDLProc controllernode workernode; do
			mem[$i]=`ps aux | grep $process | grep -v grep | awk '{print $4}'`
			let i++
		done
	fi
	echo "$runId|$test|$dt|${mem[0]}|${mem[1]}|${mem[2]}|${mem[3]}|${mem[4]}|${mem[5]}|${mem[6]}|" 
	sleep 1
        if [ -f /tmp/stop.$test.txt ];then
                rm -f /tmp/stop.$test.txt
                exit
        fi
done
