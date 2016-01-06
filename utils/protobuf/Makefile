#******************************************************************************************
# $Id$
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
include ../../rules.mak
.PHONY: install clean test docs coverage leakcheck

PROGRAM=libprotobuf.so
VERSION=1.0.0
LIBRARY=$(PROGRAM).$(VERSION)

CPPFLAGS=-I. -I$(EXPORT_ROOT)/include -I/usr/include/libxml2 -DHAVE_PTHREAD
CXXFLAGS+=$(DEBUG_FLAGS) -Wall -fpic

TLIBS=-L. -L$(EXPORT_ROOT)/lib 
GLIBS=-L$(EXPORT_ROOT)/lib 

SRCS= \
  google/protobuf/stubs/atomicops_internals_x86_gcc.cpp        \
  google/protobuf/stubs/common.cpp                             \
  google/protobuf/stubs/once.cpp                               \
  google/protobuf/stubs/stringprintf.cpp                       \
  google/protobuf/extension_set.cpp                            \
  google/protobuf/generated_message_util.cpp                   \
  google/protobuf/message_lite.cpp                             \
  google/protobuf/repeated_field.cpp                           \
  google/protobuf/wire_format_lite.cpp                         \
  google/protobuf/io/coded_stream.cpp                          \
  google/protobuf/io/zero_copy_stream.cpp                      \
  google/protobuf/io/zero_copy_stream_impl_lite.cpp            \
  google/protobuf/stubs/strutil.cpp                            \
  google/protobuf/stubs/substitute.cpp                         \
  google/protobuf/stubs/structurally_valid.cpp                 \
  google/protobuf/descriptor.cpp                               \
  google/protobuf/descriptor.pb.cpp                            \
  google/protobuf/descriptor_database.cpp                      \
  google/protobuf/dynamic_message.cpp                          \
  google/protobuf/extension_set_heavy.cpp                      \
  google/protobuf/generated_message_reflection.cpp             \
  google/protobuf/message.cpp                                  \
  google/protobuf/reflection_ops.cpp                           \
  google/protobuf/service.cpp                                  \
  google/protobuf/text_format.cpp                              \
  google/protobuf/unknown_field_set.cpp                        \
  google/protobuf/wire_format.cpp                              \
  google/protobuf/io/gzip_stream.cpp                           \
  google/protobuf/io/printer.cpp                               \
  google/protobuf/io/tokenizer.cpp                             \
  google/protobuf/io/zero_copy_stream_impl.cpp                 \
  google/protobuf/compiler/importer.cpp                        \
  google/protobuf/compiler/parser.cpp

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
	find google -name '*.h' | cpio -pudm ../../export/include

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
