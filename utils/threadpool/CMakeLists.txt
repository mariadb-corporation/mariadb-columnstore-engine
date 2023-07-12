
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(threadpool_LIB_SRCS weightedthreadpool.cpp threadpool.cpp prioritythreadpool.cpp fair_threadpool.cpp)
add_library(threadpool STATIC ${threadpool_LIB_SRCS})
add_dependencies(threadpool loggingcpp external_boost)
target_link_libraries(threadpool boost_chrono common)
install(TARGETS threadpool DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)
