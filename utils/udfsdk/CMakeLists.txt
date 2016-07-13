
include_directories( ${ENGINE_COMMON_INCLUDES}
                     ../../dbcon/mysql 
                     ../../mysql/include 
                     ../../mysql/sql 
                     ../../mysql/regex )
########### next target ###############

set(udfsdk_LIB_SRCS udfinfinidb.cpp)

add_definitions(-DMYSQL_DYNAMIC_PLUGIN)

add_library(udfsdk SHARED ${udfsdk_LIB_SRCS})

set_target_properties(udfsdk PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS udfsdk DESTINATION ${ENGINE_LIBDIR})

