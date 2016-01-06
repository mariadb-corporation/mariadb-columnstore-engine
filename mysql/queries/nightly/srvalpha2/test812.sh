#!/bin/bash
#
# Test 812.  SSB 100 extent map validation.
#
TEST=test812
STATUSTEXT="812 Import ssb100c EM validation   : " 

source ../scripts/extentMapValidation.sh

# Validate the extent map min and max values against the reference log.
validateExtentMap ssb100c editem_cp.test812.log
result=$?

if [ $result -eq 0 ]; then
        echo "$STATUSTEXT Passed" > $TEST.status
else
        echo "$STATUSTEXT Failed (diff editem_cp.log_ref editem_cp.$TEST.log for diffs)" > $TEST.status
fi

cat $TEST.status
