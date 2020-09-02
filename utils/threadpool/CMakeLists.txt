
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(threadpool_LIB_SRCS weightedthreadpool.cpp threadpool.cpp prioritythreadpool.cpp)

add_library(threadpool SHARED ${threadpool_LIB_SRCS})

add_dependencies(threadpool loggingcpp)

install(TARGETS threadpool DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)
