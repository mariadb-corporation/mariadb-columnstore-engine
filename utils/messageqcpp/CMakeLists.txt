
include_directories( ${ENGINE_COMMON_INCLUDES} )

########### next target ###############

set(messageqcpp_LIB_SRCS
    messagequeue.cpp
    messagequeuepool.cpp
    bytestream.cpp
    socketparms.cpp
    inetstreamsocket.cpp
    iosocket.cpp
    compressed_iss.cpp)

IF(CMAKE-ASAN)
   MY_CHECK_AND_SET_COMPILER_FLAG("-fsanitize=address -fno-omit-frame-pointer -fno-common -fsanitize-blacklist=/home/calpont/mariadb/buildEngine/addressblacklist.txt")
ENDIF(CMAKE-ASAN)
add_library(messageqcpp SHARED ${messageqcpp_LIB_SRCS})

set_target_properties(messageqcpp PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS messageqcpp DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)
