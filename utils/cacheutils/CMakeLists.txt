
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(cacheutils_LIB_SRCS cacheutils.cpp)

add_library(cacheutils SHARED ${cacheutils_LIB_SRCS})

install(TARGETS cacheutils DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-libs)

