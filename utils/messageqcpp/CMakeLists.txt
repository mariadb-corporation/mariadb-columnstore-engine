
target_include_directories(messageqcpp_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_library(messageqcpp SHARED 
    messagequeue.cpp
    messagequeuepool.cpp
    bytestream.cpp
    socketparms.cpp
    inetstreamsocket.cpp
    iosocket.cpp
    compressed_iss.cpp)

target_link_libraries(messageqcpp 
  PUBLIC messageqcpp_h
        brm_h
        loggingcpp
        compress
  PRIVATE common_h
          configcpp_h
          )

set_target_properties(messageqcpp PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_pch(messageqcpp)

install(TARGETS messageqcpp DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

