include_directories(${ENGINE_COMMON_INCLUDES})

set(pron_LIB_SRCS pron.cpp)
columnstore_static_library(pron ${pron_LIB_SRCS})
columnstore_link(pron PRIVATE messageqcpp loggingcpp)
