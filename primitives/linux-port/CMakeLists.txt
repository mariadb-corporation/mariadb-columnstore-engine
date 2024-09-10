INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES} ../blockcache ../primproc)

# ########## next target ###############

SET (processor_STAT_SRCS primitiveprocessor.cpp dictionary.cpp column.cpp)

ADD_LIBRARY (processor STATIC ${processor_STAT_SRCS})

ADD_DEPENDENCIES (processor loggingcpp)

TARGET_LINK_LIBRARIES (processor ${NETSNMP_LIBRARIES})

INSTALL (TARGETS processor DESTINATION ${ENGINE_LIBDIR})
