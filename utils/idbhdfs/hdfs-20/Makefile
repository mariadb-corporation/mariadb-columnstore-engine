# Copyright (C) 2014 InfiniDB, Inc.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2 of
# the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
# MA 02110-1301, USA.
#
# InfiniDB FOSS License Exception
# We want free and open source software applications under certain
# licenses to be able to use the GPL-licensed InfiniDB idbhdfs
# libraries despite the fact that not all such FOSS licenses are
# compatible with version 2 of the GNU General Public License.  
# Therefore there are special exceptions to the terms and conditions 
# of the GPLv2 as applied to idbhdfs libraries, which are 
# identified and described in more detail in the FOSS License 
# Exception in the file utils/idbhdfs/FOSS-EXCEPTION.txt
#

include ../../../rules.mak
.PHONY: install clean test docs coverage leakcheck

PLUGIN_SRC=../hdfs-shared
LIBRARY=hdfs-20.so
SCRIPTS=setenv-hdfs-20

CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2 -I.
NEW_DEBUG_FLAGS=$(shell echo "$(DEBUG_FLAGS)" | sed s/-flto//g)
CXXFLAGS+=$(NEW_DEBUG_FLAGS) -Wall -fpic -DCDH4

TLIBS=-L. -L$(EXPORT_ROOT)/lib 
GLIBS=-L$(EXPORT_ROOT)/lib 

SRCS=\
	HdfsFile.cpp \
	HdfsFileSystem.cpp \
	HdfsFsCache.cpp	\
	HdfsRdwrFileBuffer.cpp \
	HdfsRdwrMemBuffer.cpp \
	HdfsPlugin.cpp

%.o : $(PLUGIN_SRC)/%.cpp
	@mkdir -p $(dir $@)
	@echo "============="
	@echo "Compiling $<"
	@echo $(CXX) $(CXXFLAGS) $(CPPFLAGS) -c $< -o $@
	@$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c $< -o $@
        
LINCLUDES=\

OBJS=$(SRCS:.cpp=.o)

$(LIBRARY): $(OBJS)
	$(LINK.cpp) -shared -o $(LIBRARY) $(OBJS) -L. -lhdfs

install: bootstrap $(PROGRAM) $(SCRIPTS)
	umask 002; \
	mkdir -p $(INSTALL_ROOT_BIN); \
	pushd $(INSTALL_ROOT_BIN) >/dev/null; \
		rm -f $(SCRIPTS); \
	popd >/dev/null; \
	$(INSTALL) $(SCRIPTS) $(INSTALL_ROOT_BIN)
	pushd $(INSTALL_ROOT_BIN) >/dev/null; \
	chmod -f 555 $(SCRIPTS); \

	umask 002; \
	mkdir -p $(INSTALL_ROOT_LIB); \
	$(INSTALL) $(LIBRARY) $(INSTALL_ROOT_LIB)

bootstrap:

clean:
	rm -rf $(PROGRAM) $(LIBRARY) $(OBJS) \
	*~ *.o *.d* *-gcov* *.gcov \
	html config.tag

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

%.d: $(PLUGIN_SRC)/%.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) tdriver.d btdriver.d tdriver*.d
endif
