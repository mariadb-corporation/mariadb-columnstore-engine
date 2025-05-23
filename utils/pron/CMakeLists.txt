include_directories(${ENGINE_COMMON_INCLUDES})

set(COLUMNSTORE_STATIC_LIBRARIES TRUE)

set(pron_LIB_SRCS pron.cpp)
columnstore_library(pron ${pron_LIB_SRCS})
columnstore_link(pron PRIVATE messageqcpp loggingcpp)
