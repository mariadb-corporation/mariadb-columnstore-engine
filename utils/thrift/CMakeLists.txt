
cs_add_library(thrift SHARED 
    thrift/Thrift.cpp
    thrift/server/TSimpleServer.cpp
    thrift/transport/TSocket.cpp
    thrift/transport/TServerSocket.cpp
    thrift/transport/TBufferTransports.cpp
    thrift/TApplicationException.cpp)

target_compile_definitions(thrift PRIVATE THRIFT_SQUELCH_CONSOLE_OUTPUT)

set_target_properties(thrift PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_include_directories(thrift INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})


install(TARGETS thrift DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

