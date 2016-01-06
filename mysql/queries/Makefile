#******************************************************************************************
# $Id$
#
# Copyright (C) 2006 Calpont Corp
# All rights reserved
#*****************************************************************************************/
.PHONY: install clean test docs coverage leakcheck

PPROGRAM=queryTester

VERSION=1.0.0

CPPFLAGS=-I$(EXPORT_ROOT)/include
CXXFLAGS+=$(DEBUG_FLAGS) -Wall

CLIBS=-L$(EXPORT_ROOT)/lib 
LDFLAGS+=$(CLIBS)
LD_LIBRARY_PATH=

SRCS=queryTester.cpp

OBJS=$(SRCS:.cpp=.o)

all: $(PPROGRAM)

$(PPROGRAM): queryTester.o
	$(LINK.cpp) -o $@ $^

install: 

bootstrap:

clean:
	rm -f $(OBJS) $(PPROGRAM) $(LIBRARY) core *~ *.tag *-gcov.* *.gcov tdriver-gcov *.d
	rm -rf html

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

tdriver:

test:

tdriver-gcov: 

coverage:

leakcheck:
