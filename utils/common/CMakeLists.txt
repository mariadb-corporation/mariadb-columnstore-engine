INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (
  common_LIB_SRCS
  fixedallocator.cpp
  poolallocator.cpp
  cgroupconfigurator.cpp
  MonitorProcMem.cpp
  nullvaluemanip.cpp
  threadnaming.cpp
  utils_utf8.cpp
  statistics.cpp
  string_prefixes.cpp
)

ADD_LIBRARY (common SHARED ${common_LIB_SRCS})

TARGET_LINK_LIBRARIES (common boost_filesystem)

ADD_DEPENDENCIES (common loggingcpp)

INSTALL (
  TARGETS common
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
