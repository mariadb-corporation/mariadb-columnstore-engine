include_directories(${ENGINE_COMMON_INCLUDES} ../blockcache ../primproc)

# ########## next target ###############

set(processor_STAT_SRCS primitiveprocessor.cpp dictionary.cpp column.cpp)

IF(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|amd64|i386|i686")
  IF(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    SET_SOURCE_FILES_PROPERTIES(column.cpp PROPERTIES
      COMPILE_FLAGS "-msse4.2 -mpclmul")
  ENDIF()
ENDIF()

columnstore_static_library(processor ${processor_STAT_SRCS})
columnstore_link(processor ${NETSNMP_LIBRARIES} loggingcpp)
