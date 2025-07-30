include_directories(${ENGINE_COMMON_INCLUDES} ../../dbcon/mysql)

set(udfsdk_LIB_SRCS
    udfsdk.cpp
    mcsv1_udaf.cpp
    allnull.cpp
    ssq.cpp
    avg_mode.cpp
    avgx.cpp
    distinct_count.cpp
)

columnstore_library(udfsdk ${udfsdk_LIB_SRCS})
columnstore_link(udfsdk PRIVATE loggingcpp)

# Do anyone use it?
add_definitions(-DMYSQL_DYNAMIC_PLUGIN)
set(udf_mysql_LIB_SRCS udfmysql.cpp)
columnstore_mysql_plugin_library(udf_mysql SHARED ${udf_mysql_LIB_SRCS})
