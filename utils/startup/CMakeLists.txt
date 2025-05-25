include_directories(${ENGINE_COMMON_INCLUDES})

set(idbboot_LIB_SRCS installdir.cpp)
columnstore_static_library(idbboot ${idbboot_LIB_SRCS})
add_dependencies(idbboot external_boost)
