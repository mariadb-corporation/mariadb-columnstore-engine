#!/bin/sh

#ddl-gram.cpp: ddl.y
$1 -y -l -v -d -p ddl -o ddl-gram-temp.cpp ddl.y
set +e; \
if [ -f ddl-gram.cpp ]; \
	then diff -abBq ddl-gram-temp.cpp ddl-gram.cpp >/dev/null 2>&1; \
	if [ $? -ne 0 ]; \
		then mv -f ddl-gram-temp.cpp ddl-gram.cpp; \
		else touch ddl-gram.cpp; \
	fi; \
	else mv -f ddl-gram-temp.cpp ddl-gram.cpp; \
fi
set +e; \
if [ -f ddl-gram.h ]; \
	then diff -abBq ddl-gram-temp.hpp ddl-gram.h >/dev/null 2>&1; \
	if [ $? -ne 0 ]; \
		then mv -f ddl-gram-temp.hpp ddl-gram.h; \
		else touch ddl-gram.h; \
	fi; \
	else mv -f ddl-gram-temp.hpp ddl-gram.h; \
fi
rm -f ddl-gram-temp.cpp ddl-gram-temp.hpp ddl-gram-temp.output



