include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(dataconvert_LIB_SRCS dataconvert.cpp)

columnstore_library(dataconvert ${dataconvert_LIB_SRCS})
columnstore_link(dataconvert PUBLIC loggingcpp datatypes)
