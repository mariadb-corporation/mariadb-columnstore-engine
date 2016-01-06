#******************************************************************************************
# $Id$
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
include ../../rules.mak
.PHONY: install clean test docs coverage leakcheck

PROGRAM=libthrift.so
VERSION=1.0.0
LIBRARY=$(PROGRAM).$(VERSION)

CPPFLAGS=-I. -I$(EXPORT_ROOT)/include -I/usr/include/libxml2 -DTHRIFT_SQUELCH_CONSOLE_OUTPUT
CXXFLAGS+=$(DEBUG_FLAGS) -Wall -fpic

TLIBS=-L. -L$(EXPORT_ROOT)/lib 
GLIBS=-L$(EXPORT_ROOT)/lib 

SRCS= \
  thrift/transport/TSocket.cpp \
  thrift/transport/TBufferTransports.cpp \
  thrift/TApplicationException.cpp \
  thrift/Thrift.cpp \
  thrift/transport/TServerSocket.cpp \
  thrift/server/TSimpleServer.cpp

OBJS=$(SRCS:.cpp=.o)

$(LIBRARY): $(OBJS)
	$(LINK.cpp) -shared -o $(LIBRARY) $(OBJS)
	rm -f $(PROGRAM)
	ln -s $(LIBRARY) $(PROGRAM)

install: bootstrap $(LIBRARY)
	umask 002; \
	mkdir -p $(INSTALL_ROOT_LIB) $(INSTALL_ROOT_INCLUDE); \
	pushd $(INSTALL_ROOT_LIB) >/dev/null; \
		rm -f $(LIBRARY) $(PROGRAM); \
		ln -s $(LIBRARY) $(PROGRAM); \
	popd >/dev/null; \
	$(INSTALL) $(LIBRARY) $(INSTALL_ROOT_LIB)

bootstrap:
	mkdir -p ../../export/include; \
	find thrift -name '*.h' -o -name '*.tcc' | cpio -pudm ../../export/include

clean:
	rm -rf $(PROGRAM) $(LIBRARY) $(OBJS) *~ *.o *.d* *-gcov* *.gcov

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d)
endif
