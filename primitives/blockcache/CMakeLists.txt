INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES} ../primproc)

# ########## next target ###############

SET (
  dbbc_STAT_SRCS
  blockcacheclient.cpp
  blockrequestprocessor.cpp
  fileblockrequestqueue.cpp
  filebuffer.cpp
  filebuffermgr.cpp
  filerequest.cpp
  iomanager.cpp
  stats.cpp
  fsutils.cpp
)

# libdbbc_a_CXXFLAGS = $(march_flags) $(AM_CXXFLAGS)

ADD_LIBRARY (dbbc STATIC ${dbbc_STAT_SRCS})

ADD_DEPENDENCIES (dbbc loggingcpp)

TARGET_LINK_LIBRARIES (dbbc ${NETSNMP_LIBRARIES})

INSTALL (TARGETS dbbc DESTINATION ${ENGINE_LIBDIR})
