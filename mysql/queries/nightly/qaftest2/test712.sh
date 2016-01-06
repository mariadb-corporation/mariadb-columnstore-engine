#!/bin/bash
#
# Test 712.  TPCH 100 extent map validation.
#
TEST=test712
STATUSTEXT="712 Import tpch100c EM validation  : " 

source ../scripts/extentMapValidation.sh

validateExtentMap tpch100c editem_cp.$TEST.log
result=$?

if [ $result -eq 0 ]; then
        echo "$STATUSTEXT Passed" > $TEST.status
else
        echo "$STATUSTEXT Failed (diff editem_cp.log.ref editem_cp.$TEST.log for diffs)" > $TEST.status
fi

cat $TEST.status
