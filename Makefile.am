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

# $Id$

ACLOCAL_AMFLAGS = -I m4
# FIXME:
# this sets the local linker path (-rpath) so that the ColumnStore
# libraries and binaries know where to look for the necessary
# libraries.  This is almost certainly a wrong approach, as 
# MySQL should look in the plugin path for libraries, but that
# path comes from CMake, not from autotools.  For now, until
# the ColumnStore engine is integrated with CMake into the 
# MariaDB server, the prefix must be set the same for the
# MariaDB compilation and the engine compilation.  I can't see
# a way around that for now.
#
# FIXME:
# a second problem here is that this manually specifies 
# the realtime library and a custom boost library. These
# should be detected and used automatically by the autotools
# process.  A custom boost library should not be needed
# either. The system boost should be used. I am not aware if
# there are changes made to this custom boost, so I am leaving
# this AS IS for now.
AM_LIBTOOLFLAGS = -Wl,-rpath -Wl,$(prefix)/lib -lrt -lboost_idb
.PHONY: test coverage leakcheck docs bootstrap install net-snmp-install

net-snmp/net-snmp-5.7.3/agent/.libs/libnetsnmpmibs.so:
	cd net-snmp/net-snmp; \
	./configure --prefix=$(prefix) --with-mib-modules=disman/event-mib --with-cc=gcc \
		--with-logfile=$(prefix)/log/snmpd.log \
		--with-sys-location=Unknown \
		--with-sys-contact=root@localhost.localdomain \
		--with-default-snmp-version=3 \
		--with-persistent-directory=$(prefix)var/net-snmp \
		--without-openssl \
		--with-ldflags="-Wl,-rpath -Wl,$(prefix)/lib"; \
		$(MAKE) 

all-local: net-snmp/net-snmp-5.7.3/agent/.libs/libnetsnmpmibs.so
	$(MAKE) -C utils
	$(MAKE) -C oam/oamcpp
	$(MAKE) -C snmpd
	$(MAKE) -C dbcon/execplan
	$(MAKE) -C dbcon/joblist
	$(MAKE) -C versioning
	$(MAKE) -C oam
	$(MAKE) -C versioning dbrm tools
	$(MAKE) -C writeengine/wrapper
	$(MAKE) -C writeengine/client
	$(MAKE) -C writeengine/xml
	$(MAKE) -C writeengine/redistribute
	$(MAKE) -C dbcon/ddlpackage
	$(MAKE) -C dbcon/ddlpackageproc
	$(MAKE) -C dbcon/dmlpackage
	$(MAKE) -C dbcon/dmlpackageproc
	$(MAKE) -C dbcon
	$(MAKE) -C exemgr
	$(MAKE) -C ddlproc
	$(MAKE) -C dmlproc
	$(MAKE) -C procmon
	$(MAKE) -C procmgr
	$(MAKE) -C oamapps
	$(MAKE) -C decomsvr
	$(MAKE) -C primitives
	$(MAKE) -C tools
	$(MAKE) -C writeengine/server
	$(MAKE) -C writeengine/bulk
	$(MAKE) -C writeengine/splitter
	echo $(idb_cxxflags) $(CXXFLAGS) $(DEBUG_FLAGS) > buildFlags

net-snmp-install:
	$(MAKE) -C net-snmp install

install: net-snmp-install
	$(MAKE) -C utils install
	$(MAKE) -C oam/oamcpp install
	$(MAKE) -C snmpd install
	$(MAKE) -C dbcon/execplan install
	$(MAKE) -C dbcon/joblist install
	$(MAKE) -C versioning install
	$(MAKE) -C oam install
	$(MAKE) -C versioning dbrm tools install
	$(MAKE) -C writeengine/wrapper install
	$(MAKE) -C writeengine/client install
	$(MAKE) -C writeengine/xml install
	$(MAKE) -C writeengine/redistribute install
	$(MAKE) -C dbcon/ddlpackage install
	$(MAKE) -C dbcon/ddlpackageproc install
	$(MAKE) -C dbcon/dmlpackage install
	$(MAKE) -C dbcon/dmlpackageproc install
	$(MAKE) -C dbcon install
	$(MAKE) -C exemgr install
	$(MAKE) -C ddlproc install
	$(MAKE) -C dmlproc install
	$(MAKE) -C procmon install
	$(MAKE) -C procmgr install
	$(MAKE) -C oamapps install
	$(MAKE) -C decomsvr install
	$(MAKE) -C primitives install
	$(MAKE) -C tools install
	$(MAKE) -C writeengine/server install
	$(MAKE) -C writeengine/bulk install
	$(MAKE) -C writeengine/splitter install

compile: all-local

test:

coverage:

leakcheck:

docs:

clean: 
	rm -f net-snmp.built; \
	find . -name \*.d.* -exec rm -f {} \; -print 
	find . -name \*.so  -exec rm -f {} \; -print
	find . -name \*.a   -exec rm -f {} \; -print
	find . -name \*.o   -exec rm -f {} \; -print
	find . -name \*.lib -exec rm -f {} \; -print
	find . -name \*.sa  -exec rm -f {} \; -print
	find . -name \*.lo  -exec rm -f {} \; -print
	find . -name \*.la  -exec rm -f {} \; -print

