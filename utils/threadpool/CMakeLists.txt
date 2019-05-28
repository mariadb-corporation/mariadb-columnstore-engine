
target_include_directories(threadpool_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_library(threadpool SHARED weightedthreadpool.cpp threadpool.cpp prioritythreadpool.cpp)

target_link_libraries(threadpool
  PUBLIC threadpool_h
  PRIVATE Boost::thread
          loggingcpp_h 
          primproc_h 
          messageqcpp_h 
          common_h 
          joblist_h 
          execplan_h)

set_target_properties(threadpool PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_pch(threadpool)

install(TARGETS threadpool DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)
