#!/bin/sh

#dml-scan.cpp: dml.l
   $1 -i -L -Pdml -odml-scan-temp.cpp dml.l
   set +e; \
   if [ -f dml-scan.cpp ]; \
       then diff -abBq dml-scan-temp.cpp dml-scan.cpp >/dev/null 2>&1; \
       if [ $$? -ne 0 ]; \
           then mv -f dml-scan-temp.cpp dml-scan.cpp; \
           else touch dml-scan.cpp; \
       fi; \
       else mv -f dml-scan-temp.cpp dml-scan.cpp; \
   fi
   rm -f dml-scan-temp.cpp