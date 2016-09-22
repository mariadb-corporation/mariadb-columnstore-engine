
include_directories( ${ENGINE_COMMON_INCLUDES} )

########### next target ###############

set(compress_LIB_SRCS
    idbcompress.cpp
    snappy.cpp
    snappy-sinksource.cpp
    version1.cpp
    snappy-stubs-internal.cpp)

add_definitions(-DNDEBUG)

add_library(compress SHARED ${compress_LIB_SRCS})

set_target_properties(compress PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS compress DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

