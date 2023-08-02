
include_directories( ${ENGINE_COMMON_INCLUDES} )

########### next target ###############

set(messageqcpp_LIB_SRCS
    messagequeue.cpp
    messagequeuepool.cpp
    bytestream.cpp
    socketparms.cpp
    inetstreamsocket.cpp
    iosocket.cpp
    compressed_iss.cpp
    bytestreampool.cpp
)

add_library(messageqcpp SHARED ${messageqcpp_LIB_SRCS})

add_dependencies(messageqcpp loggingcpp)

install(TARGETS messageqcpp DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)
