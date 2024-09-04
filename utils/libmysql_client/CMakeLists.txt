INCLUDE_DIRECTORIES (BEFORE ${LIBMARIADB_BININC_DIR} ${LIBMARIADB_SRCINC_DIR})
ADD_DEFINITIONS (-DMYSQL_SERVICE_THD_TIMEZONE_INCLUDED)

INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

SET (libmysql_client_LIB_SRCS libmysql_client.cpp)

IF (COLUMNSTORE_ALL_STATIC)
  ADD_LIBRARY (libmysql_client STATIC ${libmysql_client_LIB_SRCS})
ELSE ()
  ADD_LIBRARY (libmysql_client SHARED ${libmysql_client_LIB_SRCS})
ENDIF ()

TARGET_LINK_LIBRARIES (libmysql_client ${MARIADB_CLIENT_LIBS})

ADD_DEPENDENCIES (libmysql_client loggingcpp)

INSTALL (
  TARGETS libmysql_client
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)
