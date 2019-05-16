
include_directories( ${ENGINE_COMMON_INCLUDES} )

########### next target ###############

set(libmysql_client_LIB_SRCS libmysql_client.cpp)

add_library(libmysql_client SHARED ${libmysql_client_LIB_SRCS})

target_link_libraries(libmysql_client PUBLIC loggingcpp)

set_target_properties(libmysql_client PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS libmysql_client DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

