#!/bin/bash
script_dir=$(dirname $0)

for i in q*.sql; do
	$script_dir/diff.sh $i > /dev/null
	if [ $? -eq 0 ]; then
		echo "$i matches."
	else
		echo "$i does not match."
	fi
done
