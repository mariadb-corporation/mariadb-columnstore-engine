include_directories(${ENGINE_COMMON_INCLUDES} ../blockcache ../primproc)

# ########## next target ###############

set(processor_STAT_SRCS primitiveprocessor.cpp dictionary.cpp column.cpp)

columnstore_static_library(processor ${processor_STAT_SRCS})
columnstore_link(processor loggingcpp)
