include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(batchloader_LIB_SRCS batchloader.cpp)

columnstore_library(batchloader ${batchloader_LIB_SRCS})

add_dependencies(batchloader loggingcpp)

columnstore_link(batchloader ${NETSNMP_LIBRARIES})
