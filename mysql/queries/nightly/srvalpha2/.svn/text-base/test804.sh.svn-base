#!/bin/bash
#
# Test 804.  Ssb100 extent map validation.
#
TEST=test804
STATUSTEXT="804 Query ssb100 EM validation     : " 

source ../scripts/extentMapValidation.sh

validateExtentMap ssb100 editem_cp.$TEST.log
result=$?

if [ $result -eq 0 ]; then
        echo "$STATUSTEXT Passed" > $TEST.status
else
        echo "$STATUSTEXT Failed (diff editem_cp.log.ref editem_cp.$TEST.log for diffs)" > $TEST.status
fi

cat $TEST.status
