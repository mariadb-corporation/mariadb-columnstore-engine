include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(cacheutils_LIB_SRCS cacheutils.cpp)

columnstore_library(cacheutils ${cacheutils_LIB_SRCS})

add_dependencies(cacheutils loggingcpp)

install(TARGETS cacheutils DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)
