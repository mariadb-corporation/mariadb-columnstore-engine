#!/bin/bash
#
# Test 296.  Validate EM Min/Max information against dmlc schema.
#

. ../scripts/common.sh

TEST=test296
STATUSTEXT="296 DMLC schema EM Validation:       "

echo "$STATUSTEXT In Progress" > $TEST.status

DB=dmlc

../../../../tools/calpontSupport/minMaxCheck.sh $DB > $TEST.log 2>&1
rrn=$?
errs=`grep -i error $TEST.log | wc -l`

# 
# Populate the .status file.
#
if [ $? -ne 0 ] || [ $errs -gt 0 ]; then
        echo "$STATUSTEXT Failed (check $TEST.log)" > $TEST.status
		ret=1
else
        echo "$STATUSTEXT Passed" > $TEST.status
		ret=0
fi

cat $TEST.status
exit $ret

