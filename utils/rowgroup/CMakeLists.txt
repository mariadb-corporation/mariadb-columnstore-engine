include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(rowgroup_LIB_SRCS rowaggregation.cpp rowgroup.cpp rowstorage.cpp)

columnstore_library(rowgroup ${rowgroup_LIB_SRCS})
columnstore_link(rowgroup PRIVATE ${NETSNMP_LIBRARIES} funcexp loggingcpp)
add_dependencies(rowgroup external_boost)
