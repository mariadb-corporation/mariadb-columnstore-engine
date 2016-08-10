
include_directories(${KDE4_INCLUDES} ${KDE4_INCLUDE_DIR} ${QT_INCLUDES} )


########### next target ###############

set(brmtest_SRCS brmtest.cpp locks.cpp)

kde4_add_executable(brmtest ${brmtest_SRCS})

target_link_libraries(brmtest ${KDE4_KDECORE_LIBS} execplan joblist rowgroup dataconvert loggingcpp @boost_thread_lib@ brm messageqcpp rwlock configcpp xml2 joiner oamcpp snmpmanager @boost_filesystem_lib@ @boost_date_time_lib@ multicast funcexp)

install(TARGETS brmtest ${INSTALL_TARGETS_DEFAULT_ARGS})


########### install files ###############




#original Makefile.am contents follow:

## $Id: Makefile.am 333 2009-04-03 20:35:04Z rdempsey $
### Process this file with automake to produce Makefile.in
#
#AM_CPPFLAGS = $(idb_cppflags)
#AM_CFLAGS = $(idb_cflags)
#AM_CXXFLAGS = $(idb_cxxflags)
#AM_LDFLAGS = $(idb_ldflags)
#bin_PROGRAMS = brmtest
#brmtest_SOURCES = brmtest.cpp locks.cpp
#brmtest_CPPFLAGS = -I@prefix@/Calpont/include -I/usr/include/libxml2 $(AM_CPPFLAGS)
#brmtest_LDFLAGS = @idb_common_ldflags@ -lexecplan -ljoblist -lrowgroup -ldataconvert -lloggingcpp -l@boost_thread_lib@ -lbrm -lmessageqcpp -lrwlock -lconfigcpp -lxml2 -ljoiner \
#-loamcpp -lsnmpmanager -l@boost_filesystem_lib@ -l@boost_date_time_lib@ @netsnmp_libs@ -lmulticast -lfuncexp $(AM_LDFLAGS)
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
