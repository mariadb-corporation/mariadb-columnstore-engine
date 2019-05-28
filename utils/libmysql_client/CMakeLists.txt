
target_include_directories(libmysql_client_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_library(libmysql_client SHARED libmysql_client.cpp)

set_target_properties(libmysql_client PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_link_libraries(libmysql_client 
  PUBLIC libmysql_client_h mariadb 
  PRIVATE messageqcpp_h
          loggingcpp_h
          configcpp_h)

install(TARGETS libmysql_client DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)
