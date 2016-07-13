
include_directories(${KDE4_INCLUDES} ${KDE4_INCLUDE_DIR} ${QT_INCLUDES} )


########### next target ###############

set(evalidx_SRCS evalidx.cpp)

kde4_add_executable(evalidx ${evalidx_SRCS})

target_link_libraries(evalidx ${KDE4_KDECORE_LIBS} dmlpackageproc execplan joblist rowgroup writeengine brm dataconvert cacheutils dmlpackage messageqcpp loggingcpp configcpp rwlock @boost_thread_lib@ xml2 joiner oamcpp snmpmanager @boost_filesystem_lib@ @boost_date_time_lib@ multicast funcexp)

install(TARGETS evalidx ${INSTALL_TARGETS_DEFAULT_ARGS})


########### install files ###############




#original Makefile.am contents follow:

## $Id: Makefile.am 333 2009-04-03 20:35:04Z rdempsey $
### Process this file with automake to produce Makefile.in
#
#AM_CPPFLAGS = $(idb_cppflags)
#AM_CFLAGS = $(idb_cflags)
#AM_CXXFLAGS = $(idb_cxxflags)
#AM_LDFLAGS = $(idb_ldflags)
#bin_PROGRAMS = evalidx
#evalidx_SOURCES = evalidx.cpp
#evalidx_CPPFLAGS = @idb_common_includes@ $(AM_CPPFLAGS)
#evalidx_LDFLAGS = @idb_common_ldflags@ -ldmlpackageproc -lexecplan -ljoblist -lrowgroup -lwriteengine -lbrm \
#-ldataconvert -lcacheutils -ldmlpackage -lmessageqcpp -lloggingcpp -lconfigcpp -lrwlock -l@boost_thread_lib@ -lxml2 \
#-ljoiner -loamcpp -lsnmpmanager -l@boost_filesystem_lib@ -l@boost_date_time_lib@ @netsnmp_libs@ -lmulticast -lfuncexp \
#$(AM_LDFLAGS)
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
