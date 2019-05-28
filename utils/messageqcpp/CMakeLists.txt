
target_include_directories(messageqcpp_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

cs_add_library(messageqcpp STATIC 
    messagequeue.cpp
    messagequeuepool.cpp
    bytestream.cpp
    socketparms.cpp
    inetstreamsocket.cpp
    iosocket.cpp
    compressed_iss.cpp
	)

target_link_libraries(messageqcpp 
	INTERFACE messageqcpp_h
  PUBLIC    
  PRIVATE
    columnstore_h
    configcpp_h
    loggingcpp_h
    compress_h
  )




