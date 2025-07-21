include_directories(${ENGINE_COMMON_INCLUDES})

set(messageqcpp_LIB_SRCS
    bytestream.cpp
    bytestreampool.cpp
    compressed_iss.cpp
    inetstreamsocket.cpp
    iosocket.cpp
    messagequeue.cpp
    messagequeuepool.cpp
    socketparms.cpp
)

columnstore_static_library(messageqcpp ${messageqcpp_LIB_SRCS})
columnstore_link(messageqcpp PUBLIC loggingcpp compress)
