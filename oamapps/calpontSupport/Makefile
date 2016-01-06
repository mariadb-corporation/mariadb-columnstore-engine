#******************************************************************************************
# $Id$
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
include ../../rules.mak
.PHONY: install clean test docs coverage leakcheck

SCRIPTS=hardwareReport.sh softwareReport.sh configReport.sh logReport.sh resourceReport.sh alarmReport.sh bulklogReport.sh hadoopReport.sh

PROGRAM=calpontSupport

VERSION=1.0.0

CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2
CXXFLAGS+=$(DEBUG_FLAGS) -Wall

CLIBS=-L$(EXPORT_ROOT)/lib -L/usr/local/Calpont/lib $(IDB_COMMON_LIBS) $(IDB_SNMP_LIBS)
LDFLAGS+=$(CLIBS) -Wl,--rpath -Wl,/usr/local/Calpont/lib

SRCS=calpontSupport.cpp

OBJS=$(SRCS:.cpp=.o)

all: $(PROGRAM)

$(PROGRAM): calpontSupport.o
	$(LINK.cpp) -o $@ $^ -Wl,-Bstatic -lreadline -Wl,-Bdynamic -lncurses -ltinfo

install: bootstrap $(PROGRAM)
	umask 002; \
	mkdir -p $(INSTALL_ROOT_BIN)
	pushd $(INSTALL_ROOT_BIN) >/dev/null; \
	rm -f $(SCRIPTS); \
	popd >/dev/null; \
	$(INSTALL) $(PROGRAM) $(SCRIPTS) $(INSTALL_ROOT_BIN)
	pushd $(INSTALL_ROOT_BIN) >/dev/null; \
	chmod -f 755 $(PROGRAM) $(SCRIPTS); \
	popd >/dev/null


bootstrap:

clean:
	rm -f $(OBJS) tdriver.o $(PROGRAM) $(LIBRARY) tdriver core *~ *.tag *-gcov.* *.gcov tdriver-gcov *.d *.d.*
	rm -rf html

docs:

tdriver:

test:

coverage:

leakcheck:

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) tdriver.d
endif
