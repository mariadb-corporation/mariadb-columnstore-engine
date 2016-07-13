#!/bin/sh

#ddl-scan.cpp: ddl.l
$1 -i -L -P ddl -o ddl-scan-temp.cpp ddl.l
set +e;
if [ -f ddl-scan.cpp ];
	then diff -abBq ddl-scan-temp.cpp ddl-scan.cpp >/dev/null 2>&1;
	if [ "$?" -ne 0 ];
		then mv -f ddl-scan-temp.cpp ddl-scan.cpp;
		else touch ddl-scan.cpp;
	fi;
	else mv -f ddl-scan-temp.cpp ddl-scan.cpp;
fi
rm -f ddl-scan-temp.cpp


