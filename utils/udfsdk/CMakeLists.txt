INCLUDE_DIRECTORIES (${ENGINE_COMMON_INCLUDES} ../../dbcon/mysql)

# ########## next target ###############

SET (
  udfsdk_LIB_SRCS
  udfsdk.cpp
  mcsv1_udaf.cpp
  allnull.cpp
  ssq.cpp
  avg_mode.cpp
  avgx.cpp
  distinct_count.cpp
)

ADD_DEFINITIONS (-DMYSQL_DYNAMIC_PLUGIN)

IF (COLUMNSTORE_ALL_STATIC)
  ADD_LIBRARY (udfsdk STATIC ${udfsdk_LIB_SRCS})
ELSE ()
  ADD_LIBRARY (udfsdk SHARED ${udfsdk_LIB_SRCS})
ENDIF ()

ADD_DEPENDENCIES (udfsdk loggingcpp)

INSTALL (
  TARGETS udfsdk
  DESTINATION ${ENGINE_LIBDIR}
  COMPONENT columnstore-engine
)

SET (udf_mysql_LIB_SRCS udfmysql.cpp)

ADD_LIBRARY (udf_mysql SHARED ${udf_mysql_LIB_SRCS})

INSTALL (
  TARGETS udf_mysql
  DESTINATION ${MARIADB_PLUGINDIR}
  COMPONENT columnstore-engine
)
