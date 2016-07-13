#!/bin/sh

#dml-gram.cpp: dml.y
$1 -l -v -d -p dml -o dml-gram-temp.cpp dml.y
   set +e; \
   if [ -f dml-gram.cpp ]; \
       then diff -abBq dml-gram-temp.cpp dml-gram.cpp >/dev/null 2>&1; \
       if [ $? -ne 0 ]; \
           then mv -f dml-gram-temp.cpp dml-gram.cpp; \
           else touch dml-gram.cpp; \
       fi; \
       else mv -f dml-gram-temp.cpp dml-gram.cpp; \
   fi
   set +e; \
   if [ -f dml-gram.h ]; \
       then diff -abBq dml-gram-temp.hpp dml-gram.h >/dev/null 2>&1; \
       if [ $? -ne 0 ]; \
           then mv -f dml-gram-temp.hpp dml-gram.h; \
           else touch dml-gram.h; \
       fi; \
       else mv -f dml-gram-temp.hpp dml-gram.h; \
   fi
   rm -f dml-gram-temp.cpp dml-gram-temp.hpp dml-gram-temp.output