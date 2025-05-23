include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(joiner_LIB_SRCS tuplejoiner.cpp joinpartition.cpp)

columnstore_library(joiner ${joiner_LIB_SRCS})
columnstore_link(joiner PRIVATE loggingcpp)
