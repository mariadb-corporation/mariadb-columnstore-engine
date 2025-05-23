include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(oamcpp_LIB_SRCS liboamcpp.cpp oamcache.cpp)

set(COLUMNSTORE_STATIC_LIBRARIES YES)

columnstore_library(oamcpp ${oamcpp_LIB_SRCS})
columnstore_link(oamcpp loggingcpp)

target_compile_options(oamcpp PRIVATE -Wno-unused-result)
