INCLUDE_DIRECTORIES (${KDE4_INCLUDES} ${KDE4_INCLUDE_DIR} ${QT_INCLUDES})

# ########## next target ###############

SET (cfread_SRCS cfread.cpp)

KDE4_ADD_EXECUTABLE (cfread ${cfread_SRCS})

TARGET_LINK_LIBRARIES (
  cfread
  ${KDE4_KDECORE_LIBS}
  pthread
  writeengine
  brm
  rwlock
  messageqcpp
  dl
  configcpp
  xml2
  loggingcpp
  cacheutils
  boost_idb
  pthread
  rt
)

INSTALL (TARGETS cfread ${INSTALL_TARGETS_DEFAULT_ARGS})

# ########## next target ###############

SET (mtread_SRCS mtread.cpp)

KDE4_ADD_EXECUTABLE (mtread ${mtread_SRCS})

TARGET_LINK_LIBRARIES (
  mtread
  ${KDE4_KDECORE_LIBS}
  pthread
  writeengine
  brm
  rwlock
  messageqcpp
  dl
  configcpp
  xml2
  loggingcpp
  cacheutils
  boost_idb
  pthread
  rt
)

INSTALL (TARGETS mtread ${INSTALL_TARGETS_DEFAULT_ARGS})

# ########## install files ###############

# original Makefile.am contents follow:

# $Id: Makefile.am 333 2009-04-03 20:35:04Z rdempsey $ Process this file with automake to produce Makefile.in
#
# AM_CPPFLAGS = $(idb_cppflags) AM_CFLAGS = $(idb_cflags) AM_CXXFLAGS = $(idb_cxxflags) AM_LDFLAGS = $(idb_ldflags)
# bin_PROGRAMS = cfread mtread cfread_SOURCES = cfread.cpp cfread_CPPFLAGS = @idb_common_includes@ $(AM_CPPFLAGS)
# cfread_LDFLAGS = @idb_common_ldflags@ -lpthread -lwriteengine -lbrm -lrwlock -lmessageqcpp -ldl -lconfigcpp -lxml2
# -lloggingcpp -lcacheutils -lboost_idb -lpthread -lrt $(AM_LDFLAGS) mtread_SOURCES = mtread.cpp mtread_CPPFLAGS =
# @idb_common_includes@ $(AM_CPPFLAGS) mtread_LDFLAGS = @idb_common_ldflags@ -lpthread -lwriteengine -lbrm -lrwlock
# -lmessageqcpp -ldl -lconfigcpp -lxml2 -lloggingcpp -lcacheutils -lboost_idb -lpthread -lrt $(AM_LDFLAGS)
#
# test:
#
# coverage:
#
# leakcheck:
#
# docs:
#
# bootstrap: install-data-am
#
