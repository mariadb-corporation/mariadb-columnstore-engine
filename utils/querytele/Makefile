#******************************************************************************************
# $Id$
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
include ../../rules.mak
.PHONY: install clean test docs coverage leakcheck thrift

PROGRAM=libquerytele.so
VERSION=1.0.0
LIBRARY=$(PROGRAM).$(VERSION)

CPPFLAGS=-I. -I$(EXPORT_ROOT)/include -I/usr/include/libxml2
CXXFLAGS+=$(DEBUG_FLAGS) -Wall -fpic

CLIBS=-L$(EXPORT_ROOT)/lib -lxml2 -lrt
TLIBS=-L. -lquerytele -lthrift -lboost_idb $(CLIBS)
GLIBS=$(TLIBS)

LLIBS=-L$(CALPONT_LIBRARY_PATH) -lxml2

SRCS=querytele.cpp \
	queryteleclient.cpp \
	querytele_constants.cpp \
	querytele_types.cpp \
	QueryTeleService.cpp \
	queryteleprotoimpl.cpp
LINCLUDES=querytele.h \
	queryteleclient.h \
	queryteleserverparms.h \
	querystepparms.h \
	telestats.h

OBJS=$(SRCS:.cpp=.o)

$(LIBRARY): $(OBJS)
	$(LINK.cpp) -shared -o $(LIBRARY) $(OBJS)
	rm -f $(PROGRAM)
	ln -s $(LIBRARY) $(PROGRAM)

thrift:
	thrift -out . --gen cpp querytele.thrift

install: bootstrap $(LIBRARY)
	umask 002; \
	mkdir -p $(INSTALL_ROOT_LIB) $(INSTALL_ROOT_INCLUDE); \
	pushd $(INSTALL_ROOT_LIB) >/dev/null; \
		rm -f $(LIBRARY) $(PROGRAM); \
		ln -s $(LIBRARY) $(PROGRAM); \
	popd >/dev/null; \
	$(INSTALL) $(LIBRARY) $(INSTALL_ROOT_LIB)

bootstrap:
	$(INSTALL) $(LINCLUDES) $(INSTALL_ROOT_INCLUDE)

clean:
	rm -f $(OBJS) tdriver*.o $(PROGRAM) $(LIBRARY) tdriver core *~ *.tag *-gcov.* *.gcov tdriver*-gcov *.d *.d.*
	rm -rf html serverdriver.o serverdriver

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

tdriver: tdriver.o
	$(LINK.cpp) -o $@ $^ $(TLIBS)

serverdriver: serverdriver.o
	$(LINK.cpp) -o $@ $^ $(TLIBS)

test: $(LIBRARY) tdriver
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib:/usr/local/gcc45/lib64 ./tdriver

%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

tdriver-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
tdriver-gcov: tdriver-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(GLIBS)

coverage: tdriver-gcov tdriver2-gcov
	rm -f *.gcda
	LD_LIBRARY_PATH=$(EXPORT_ROOT)/lib:/usr/local/lib:/usr/local/gcc45/lib64 ./tdriver-gcov
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file >/dev/null; \
	done
	/usr/local/bin/genCoverage.pl $(SRCS)

leakcheck: $(LIBRARY) tdriver tdriver2
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib:/usr/local/gcc45/lib64 valgrind --tool=memcheck --leak-check=yes ./tdriver

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) tdriver.d serverdriver.d
endif
