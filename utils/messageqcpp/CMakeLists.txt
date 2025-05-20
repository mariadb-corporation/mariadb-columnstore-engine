include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

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

add_library(messageqcpp STATIC ${messageqcpp_LIB_SRCS})

add_dependencies(messageqcpp loggingcpp)

# We don't isntall static library install(TARGETS messageqcpp DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)
