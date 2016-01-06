#!/bin/bash
#
# Test 704.  TPCH 100 extent map validation.
#
TEST=test704
STATUSTEXT="704 Query tpch100 EM validation    : " 

source ../scripts/extentMapValidation.sh

validateExtentMap tpch100 editem_cp.$TEST.log
result=$?

if [ $result -eq 0 ]; then
        echo "$STATUSTEXT Passed" > $TEST.status
else
        echo "$STATUSTEXT Failed (diff editem_cp.log.ref editem_cp.$TEST.log for diffs)" > $TEST.status
fi

cat $TEST.status

