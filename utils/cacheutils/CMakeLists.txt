
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(cacheutils_LIB_SRCS cacheutils.cpp)

add_library(cacheutils SHARED ${cacheutils_LIB_SRCS})

set_target_properties(cacheutils PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS cacheutils DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

