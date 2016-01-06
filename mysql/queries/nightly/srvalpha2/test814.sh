#!/bin/bash
#
# Test 814.  ssb100c extent map validation.
#
TEST=test814
STATUSTEXT="814 Query ssb100c EM validation    : " 

source ../scripts/extentMapValidation.sh

validateExtentMap ssb100 editem_cp.$TEST.log
result=$?

if [ $result -eq 0 ]; then
        echo "$STATUSTEXT Passed" > $TEST.status
else
        echo "$STATUSTEXT Failed (diff editem_cp.log.ref editem_cp.$TEST.log for diffs)" > $TEST.status
fi

cat $TEST.status
