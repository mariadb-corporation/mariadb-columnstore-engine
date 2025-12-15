#!/usr/bin/env bash

set -e

DIR_NAME=$1

echo "Checking for sanitizer reports in /$DIR_NAME/"

FOUND_ISSUES=0

# Check for ASAN reports
shopt -s nullglob
for f in /"$DIR_NAME"/asan.* /"$DIR_NAME"/asan_*; do
    if [[ -f "$f" ]]; then
        FOUND_ISSUES=1
        echo ""
        echo "=========================================="
        echo "⚠️  ASAN report found: $(basename "$f")"
        echo "=========================================="
        cat "$f"
        echo ""
    fi
done

# Check for UBSAN reports
for f in /"$DIR_NAME"/ubsan.* /"$DIR_NAME"/ubsan_*; do
    if [[ -f "$f" ]]; then
        FOUND_ISSUES=1
        echo ""
        echo "=========================================="
        echo "⚠️  UBSAN report found: $(basename "$f")"
        echo "=========================================="
        cat "$f"
        echo ""
    fi
done


if [[ $FOUND_ISSUES -eq 1 ]]; then
    exit 1
fi

echo "✅ No sanitizer reports found"
exit 0
