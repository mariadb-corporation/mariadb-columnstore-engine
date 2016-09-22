
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(threadpool_LIB_SRCS weightedthreadpool.cpp threadpool.cpp prioritythreadpool.cpp)

add_library(threadpool SHARED ${threadpool_LIB_SRCS})

set_target_properties(threadpool PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS threadpool DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)
