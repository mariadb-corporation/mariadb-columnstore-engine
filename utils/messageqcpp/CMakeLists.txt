include_directories(${ENGINE_COMMON_INCLUDES})

set(messageqcpp_LIB_SRCS
    messagequeue.cpp
    messagequeuepool.cpp
    bytestream.cpp
    socketparms.cpp
    inetstreamsocket.cpp
    samenodepseudosocket.cpp
    iosocket.cpp
    compressed_iss.cpp
    bytestreampool.cpp
)

columnstore_library(messageqcpp ${messageqcpp_LIB_SRCS})
columnstore_link(messageqcpp PUBLIC loggingcpp)
