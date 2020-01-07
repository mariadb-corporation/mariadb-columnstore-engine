
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(querytele_LIB_SRCS
    querytele.cpp
    queryteleclient.cpp
    querytele_constants.cpp
    querytele_types.cpp
    QueryTeleService.cpp
    queryteleprotoimpl.cpp)
IF(CMAKE-ASAN)
   MY_CHECK_AND_SET_COMPILER_FLAG("-fsanitize=address -fno-omit-frame-pointer -fno-common -fsanitize-blacklist=/home/calpont/mariadb/buildEngine/addressblacklist.txt")
ENDIF(CMAKE-ASAN)

add_library(querytele SHARED ${querytele_LIB_SRCS})

set_target_properties(querytele PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS querytele DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

