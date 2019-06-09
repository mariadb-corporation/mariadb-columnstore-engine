#!/bin/bash
grep -rI "^\s*#\s*include" . | cut -f 2 -d ':' | sort | uniq -c | sort -nr | grep -Ev "^\s+(1|2)\s+.*" | awk  '{ print $2, $3}'
