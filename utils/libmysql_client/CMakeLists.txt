include_directories(BEFORE ${LIBMARIADB_BININC_DIR} ${LIBMARIADB_SRCINC_DIR})
add_definitions(-DMYSQL_SERVICE_THD_TIMEZONE_INCLUDED)

include_directories(${ENGINE_COMMON_INCLUDES})

set(libmysql_client_LIB_SRCS libmysql_client.cpp)
columnstore_library(libmysql_client ${libmysql_client_LIB_SRCS})
columnstore_link(libmysql_client PRIVATE loggingcpp ${MARIADB_CLIENT_LIBS})
