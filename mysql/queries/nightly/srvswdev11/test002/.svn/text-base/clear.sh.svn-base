#!/bin/bash

count=128

if [ "x$1" == x ]; then
	arg1=0
else
	arg1="$1"
fi

if [ $arg1 -gt 0 ]; then
        count=$arg1
fi

for (( i = 1; i <= count; i++ )); do 
	rm -rf $i
done

