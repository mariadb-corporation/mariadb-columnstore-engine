#!/bin/sh
match=`/usr/local/Calpont/bin/calpontConsole getSystemNet | egrep "srvqaperf2|srvalpha2|srvprodtest1|srvalpha4" | wc -l`
if [ $match -ge 1 ]; then
	PASSWORD=qalpont!
else
	PASSWORD=Calpont1
fi
echo $PASSWORD
