include_directories(${ENGINE_COMMON_INCLUDES})

set(COLUMNSTORE_STATIC_LIBRARIES TRUE)

set(idbboot_LIB_SRCS installdir.cpp)
columnstore_library(idbboot ${idbboot_LIB_SRCS})
add_dependencies(idbboot external_boost)
