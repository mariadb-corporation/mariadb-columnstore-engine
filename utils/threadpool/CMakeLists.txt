include_directories(${ENGINE_COMMON_INCLUDES})

set(threadpool_LIB_SRCS weightedthreadpool.cpp threadpool.cpp prioritythreadpool.cpp fair_threadpool.cpp)
columnstore_library(threadpool ${threadpool_LIB_SRCS})
columnstore_link(threadpool PRIVATE boost_chrono loggingcpp)

add_dependencies(threadpool external_boost)
