
include_directories(${KDE4_INCLUDES} ${KDE4_INCLUDE_DIR} ${QT_INCLUDES} )


########### next target ###############

set(vbgen_SRCS vbgen.cpp myrand.cpp)

kde4_add_executable(vbgen ${vbgen_SRCS})

target_link_libraries(vbgen ${KDE4_KDECORE_LIBS})

install(TARGETS vbgen ${INSTALL_TARGETS_DEFAULT_ARGS})


########### install files ###############




#original Makefile.am contents follow:

## Copyright (C) 2014 InfiniDB, Inc.
##
## This program is free software; you can redistribute it and/or
## modify it under the terms of the GNU General Public License
## as published by the Free Software Foundation; version 2 of
## the License.
##
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with this program; if not, write to the Free Software
## Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
## MA 02110-1301, USA.
#
## $Id$
### Process this file with automake to produce Makefile.in
#
#AM_CPPFLAGS = $(idb_cppflags)
#AM_CFLAGS = $(idb_cflags)
#AM_CXXFLAGS = $(idb_cxxflags)
#AM_LDFLAGS = $(idb_ldflags)
#bin_PROGRAMS = vbgen
#vbgen_SOURCES = vbgen.cpp myrand.cpp
#vbgen_CPPFLAGS = $(AM_CPPFLAGS)
#vbgen_LDFLAGS = $(AM_LDFLAGS)
#
#test:
#
#coverage:
#
#leakcheck:
#
#docs:
#
#bootstrap: install-data-am
#
