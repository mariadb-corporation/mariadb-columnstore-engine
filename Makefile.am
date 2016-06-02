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

all-local: bootstrap
#	cd utils && $(MAKE) && $(MAKE) install
#	cd oam; \
#		$(MAKE) -C oamcpp install
#	cd snmpd && $(MAKE) && $(MAKE) install
#	cd dbcon; \
#		$(MAKE) -C execplan install; \
#		$(MAKE) -C joblist install
#	cd versioning && $(MAKE) && $(MAKE) install
#	cd oam && $(MAKE) && $(MAKE) install
#	cd versioning && $(MAKE) dbrm tools && $(MAKE) install_dbrm install_tools
#	cd writeengine; \
#		$(MAKE) -C wrapper install; \
#		$(MAKE) -C client install; \
#		$(MAKE) -C xml install; \
#		$(MAKE) -C redistribute install
#	cd dbcon; \
#		$(MAKE) -C ddlpackage install; \
#		$(MAKE) -C ddlpackageproc install; \
#		$(MAKE) -C dmlpackage install; \
#		$(MAKE) -C dmlpackageproc install
#	cd dbcon && $(MAKE) && $(MAKE) install
#	cd exemgr && $(MAKE) && $(MAKE) install
#	cd ddlproc && $(MAKE) && $(MAKE) install
#	cd dmlproc && $(MAKE) && $(MAKE) install
#	cd procmon && $(MAKE) && $(MAKE) install
#	cd procmgr && $(MAKE) && $(MAKE) install
#	cd oamapps && $(MAKE) && $(MAKE) install
#	cd decomsvr && $(MAKE) && $(MAKE) install
#	cd primitives && $(MAKE) && $(MAKE) install
#	cd tools && $(MAKE) && $(MAKE) install
#	cd versioning && $(MAKE) tools install_tools
#	$(MAKE) -C writeengine/server install_server
#	$(MAKE) -C writeengine/bulk install_bulk
#	$(MAKE) -C writeengine/splitter install_splitter
#	echo $(CXXFLAGS) $(DEBUG_FLAGS) > buildFlags
	cd utils && $(MAKE) 
	cd oam; \
		$(MAKE) -C oamcpp
	cd snmpd && $(MAKE)
	cd dbcon; \
		$(MAKE) -C execplan; \
		$(MAKE) -C joblist
	cd versioning && $(MAKE) 
	cd oam && $(MAKE)
	cd versioning && $(MAKE) dbrm tools && $(MAKE)_dbrm_tools
	cd writeengine; \
		$(MAKE) -C wrapper; \
		$(MAKE) -C client; \
		$(MAKE) -C xml; \
		$(MAKE) -C redistribute
	cd dbcon; \
		$(MAKE) -C ddlpackage; \
		$(MAKE) -C ddlpackageproc; \
		$(MAKE) -C dmlpackage; \
		$(MAKE) -C dmlpackageproc
	cd dbcon && $(MAKE)
	cd exemgr && $(MAKE)
	cd ddlproc && $(MAKE)
	cd dmlproc && $(MAKE)
	cd procmon && $(MAKE)
	cd procmgr && $(MAKE)
	cd oamapps && $(MAKE)
	cd decomsvr && $(MAKE)
	cd primitives && $(MAKE)
	cd tools && $(MAKE)
	cd versioning && $(MAKE) tools
	$(MAKE) -C writeengine/server
	$(MAKE) -C writeengine/bulk
	$(MAKE) -C writeengine/splitter
	echo $(CXXFLAGS) $(DEBUG_FLAGS) > buildFlags

compile: all-local

.PHONY: test coverage leakcheck docs bootstrap
test:

coverage:

leakcheck:

docs:

bootstrap:

clean: clean-am

