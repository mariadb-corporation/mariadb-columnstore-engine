include_directories(BEFORE
    ${CMAKE_BINARY_DIR}/libmariadb/include
    ${CMAKE_SOURCE_DIR}/libmariadb/include)
add_definitions(-DMYSQL_SERVICE_THD_TIMEZONE_INCLUDED)

include_directories( ${ENGINE_COMMON_INCLUDES} )

########### next target ###############

set(libmysql_client_LIB_SRCS libmysql_client.cpp)

add_library(libmysql_client SHARED ${libmysql_client_LIB_SRCS})
target_link_libraries(libmysql_client ${MARIADB_CLIENT_LIBS})

add_dependencies(libmysql_client loggingcpp)

install(TARGETS libmysql_client DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

