
include_directories( ${ENGINE_COMMON_INCLUDES}
                     ../../dbcon/mysql )
                     
########### next target ###############

set(udfsdk_LIB_SRCS udfsdk.cpp mcsv1_udaf.cpp allnull.cpp ssq.cpp avg_mode.cpp avgx.cpp distinct_count.cpp)

add_definitions(-DMYSQL_DYNAMIC_PLUGIN)

add_library(udfsdk SHARED ${udfsdk_LIB_SRCS})

add_dependencies(udfsdk loggingcpp)

install(TARGETS udfsdk DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

set(udf_mysql_LIB_SRCS udfmysql.cpp)

add_library(udf_mysql SHARED ${udf_mysql_LIB_SRCS})

install(TARGETS udf_mysql DESTINATION ${MARIADB_PLUGINDIR} COMPONENT columnstore-engine)

