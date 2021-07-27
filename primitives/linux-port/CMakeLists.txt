
include_directories( ${ENGINE_COMMON_INCLUDES} ../blockcache ../primproc)


########### next target ###############

set(processor_STAT_SRCS primitiveprocessor.cpp dictionary.cpp column.cpp)

add_library(processor STATIC ${processor_STAT_SRCS})

add_dependencies(processor loggingcpp)

target_link_libraries(processor ${NETSNMP_LIBRARIES})

INSTALL (TARGETS processor DESTINATION ${ENGINE_LIBDIR})
