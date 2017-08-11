
include_directories( ${ENGINE_COMMON_INCLUDES}
                     ../../dbcon/mysql )
                     
########### next target ###############

set(udfsdk_LIB_SRCS udfsdk.cpp mcsv1_udaf.cpp allnull.cpp ssq.cpp median.cpp avg_mode.cpp)

add_definitions(-DMYSQL_DYNAMIC_PLUGIN)

add_library(udfsdk SHARED ${udfsdk_LIB_SRCS})

set_target_properties(udfsdk PROPERTIES VERSION 1.1.0 SOVERSION 1)

install(TARGETS udfsdk DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)



set(udf_mysql_LIB_SRCS udfmysql.cpp)

add_library(udf_mysql SHARED ${udf_mysql_LIB_SRCS})

set_target_properties(udf_mysql PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS udf_mysql DESTINATION ${ENGINE_LIBDIR} COMPONENT storage-engine)

