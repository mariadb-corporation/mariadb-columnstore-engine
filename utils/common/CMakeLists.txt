
include_directories( ${ENGINE_COMMON_INCLUDES} )

########### next target ###############

set(common_LIB_SRCS
    fixedallocator.cpp
    poolallocator.cpp
    cgroupconfigurator.cpp
    MonitorProcMem.cpp
    nullvaluemanip.cpp
    threadnaming.cpp)
IF(CMAKE-ASAN)
   MY_CHECK_AND_SET_COMPILER_FLAG("-fsanitize=address -fno-omit-frame-pointer -fno-common -fsanitize-blacklist=/home/calpont/mariadb/buildEngine/addressblacklist.txt")
ENDIF(CMAKE-ASAN)

add_library(common SHARED ${common_LIB_SRCS})

add_dependencies(common loggingcpp)


set_target_properties(common PROPERTIES VERSION 1.0.0 SOVERSION 1)

install(TARGETS common DESTINATION ${ENGINE_LIBDIR} COMPONENT libs)

