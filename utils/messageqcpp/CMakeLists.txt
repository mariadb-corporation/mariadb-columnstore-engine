include_directories(${ENGINE_COMMON_INCLUDES})

set(messageqcpp_LIB_SRCS
    bytestream.cpp
    bytestreampool.cpp
    compressed_iss.cpp
    inetstreamsocket.cpp
    iosocket.cpp
    messagequeue.cpp
    messagequeuepool.cpp
    samenodepseudosocket.cpp
    socketparms.cpp
)

columnstore_library(messageqcpp ${messageqcpp_LIB_SRCS})
columnstore_link(messageqcpp PUBLIC loggingcpp compress)

columnstore_executable(messagequeue_test tdriver.cpp)
columnstore_link(
    messagequeue_test
    messageqcpp
    ${ENGINE_LDFLAGS}
    ${ENGINE_WRITE_LIBS}
    ${CPPUNIT_LIBRARIES}
    cppunit
    boost_thread
)
