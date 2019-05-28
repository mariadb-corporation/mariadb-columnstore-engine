
target_include_directories(threadpool_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

cs_add_library(threadpool STATIC
  weightedthreadpool.cpp 
  threadpool.cpp 
  prioritythreadpool.cpp
  )

target_link_libraries(threadpool
  INTERFACE threadpool_h
  PUBLIC
  PRIVATE
    loggingcpp_h
    primproc_h
    messageqcpp_h
    joblist_h
    common_h
    execplan_h
    )

#set_target_properties(threadpool PROPERTIES VERSION 1.0.0 SOVERSION 1)


#install(TARGETS threadpool DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT libs)
