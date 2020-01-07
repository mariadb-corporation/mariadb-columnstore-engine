
include_directories( ${ENGINE_COMMON_INCLUDES} )



########### next target ###############

set(rwlock_LIB_SRCS rwlock.cpp rwlock_local.cpp)
IF(CMAKE-ASAN)
   MY_CHECK_AND_SET_COMPILER_FLAG("-fsanitize=address -fno-omit-frame-pointer -fno-common -fsanitize-blacklist=/home/calpont/mariadb/buildEngine/addressblacklist.txt")
ENDIF(CMAKE-ASAN)

add_library(rwlock SHARED ${rwlock_LIB_SRCS})

set_target_properties(rwlock PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS rwlock DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

