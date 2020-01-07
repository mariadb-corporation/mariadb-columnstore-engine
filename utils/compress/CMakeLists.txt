
include_directories( ${ENGINE_COMMON_INCLUDES} ${SNAPPY_INCLUDE_DIR} )

########### next target ###############

set(compress_LIB_SRCS
    idbcompress.cpp
    version1.cpp)
IF(CMAKE-ASAN)
   MY_CHECK_AND_SET_COMPILER_FLAG("-fsanitize=address -fno-omit-frame-pointer -fno-common -fsanitize-blacklist=/home/calpont/mariadb/buildEngine/addressblacklist.txt")
ENDIF(CMAKE-ASAN)

add_definitions(-DNDEBUG)

add_library(compress SHARED ${compress_LIB_SRCS})

target_link_libraries(compress ${SNAPPY_LIBRARIES})

set_target_properties(compress PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS compress DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

