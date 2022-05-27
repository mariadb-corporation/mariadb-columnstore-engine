
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(threadpool_LIB_SRCS weightedthreadpool.cpp threadpool.cpp prioritythreadpool.cpp fair_threadpool.cpp)
add_library(threadpool SHARED ${threadpool_LIB_SRCS})
add_dependencies(threadpool loggingcpp)
install(TARGETS threadpool DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

# set(fair_threadpool_LIB_SRCS fair_threadpool.cpp)
# add_library(fair_threadpool STATIC ${fair_threadpool_LIB_SRCS})
# add_dependencies(fair_threadpool loggingcpp)
# install(TARGETS fair_threadpool DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)