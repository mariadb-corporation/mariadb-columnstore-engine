#******************************************************************************************
# $Id$
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
include ../../rules.mak
.PHONY: install clean test docs coverage leakcheck

PROGRAM=libboost_idb.so
VERSION=1.0.0
LIBRARY=$(PROGRAM).$(VERSION)

CPPFLAGS=-I. -I$(EXPORT_ROOT)/include
CXXFLAGS+=$(DEBUG_FLAGS) -Wall -fpic

TLIBS=-L. -L$(EXPORT_ROOT)/lib 
GLIBS=-L$(EXPORT_ROOT)/lib 

LLIBS=-lpthread

SRCS=\
codecvt_error_category.cpp \
cpp_regex_traits.cpp \
cregex.cpp \
c_regex_traits.cpp \
error_code.cpp \
fileiter.cpp \
future.cpp \
greg_month.cpp \
icu.cpp \
instances.cpp \
once.cpp \
operations.cpp \
path.cpp \
path_traits.cpp \
portability.cpp \
posix_api.cpp \
regex.cpp \
regex_debug.cpp \
regex_raw_buffer.cpp \
regex_traits_defaults.cpp \
static_mutex.cpp \
thread.cpp \
tss_null.cpp \
unique_path.cpp \
usinstances.cpp \
utf8_codecvt_facet.cpp \
w32_regex_traits.cpp \
wc_regex_traits.cpp \
wide_posix_api.cpp \
windows_file_codecvt.cpp \
winstances.cpp

LINCLUDES=

OBJS=$(SRCS:.cpp=.o)

$(LIBRARY): $(OBJS)
	$(LINK.cpp) -shared -o $(LIBRARY) $(OBJS) $(LLIBS)
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
	$(INSTALL) --recursive --force boost $(INSTALL_ROOT_INCLUDE)

clean:
	rm -rf $(PROGRAM) $(LIBRARY) $(OBJS) \
	*~ *.o *.d* *-gcov* *.gcov

docs:

xdocs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d)
endif
