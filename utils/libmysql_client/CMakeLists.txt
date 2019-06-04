
target_include_directories(mysql_client_h INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_library(mysql_client SHARED libmysql_client.cpp)

set_target_properties(mysql_client PROPERTIES VERSION 1.0.0 SOVERSION 1)

target_link_libraries(mysql_client 
  INTERFACE mysql_client_h 
  PRIVATE
    messageqcpp_h
    loggingcpp_h
    configcpp_h
    mariadb
    )

install(TARGETS mysql_client DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

