
include_directories( ${ENGINE_COMMON_INCLUDES} )

########### next target ###############

set(libmysql_client_LIB_SRCS libmysql_client.cpp)

add_library(libmysql_client SHARED ${libmysql_client_LIB_SRCS})

add_dependencies(libmysql_client loggingcpp)

install(TARGETS libmysql_client DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

