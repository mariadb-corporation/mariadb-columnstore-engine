include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(cacheutils_LIB_SRCS cacheutils.cpp)

columnstore_library(cacheutils ${cacheutils_LIB_SRCS})
columnstore_link(cacheutils messageqcpp boost_thread)
