include_directories(${ENGINE_COMMON_INCLUDES})
set(datatypes_LIB_SRCS mcs_int128.cpp mcs_decimal.cpp)

columnstore_library(datatypes ${datatypes_LIB_SRCS})
columnstore_link(datatypes PRIVATE loggingcpp)
add_dependencies(datatypes external_boost)
