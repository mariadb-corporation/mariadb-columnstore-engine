
include_directories( ${ENGINE_COMMON_INCLUDES} ../blockcache ../linux-port)


########### next target ###############

set(PrimProc_SRCS
    primproc.cpp
    batchprimitiveprocessor.cpp
    bppseeder.cpp
    bppsendthread.cpp
    columncommand.cpp
    command.cpp
    dictstep.cpp
    filtercommand.cpp
    logger.cpp
    passthrucommand.cpp
    primitiveserver.cpp
    pseudocc.cpp
    rtscommand.cpp
    umsocketselector.cpp
    ../../utils/common/crashtrace.cpp)

#PrimProc_CXXFLAGS = $(march_flags) $(AM_CXXFLAGS)
#IF(CMAKE-ASAN)
   MY_CHECK_AND_SET_COMPILER_FLAG("-fsanitize=address -fno-omit-frame-pointer -fno-common")
   MY_CHECK_AND_SET_COMPILER_FLAG("-fsanitize-blacklist=/home/calpont/mariadb/buildEngine/addressblacklist.txt")
#ENDIF(CMAKE-ASAN)

add_executable(PrimProc ${PrimProc_SRCS})

target_link_libraries(PrimProc ${ENGINE_LDFLAGS} ${MARIADB_CLIENT_LIBS} ${ENGINE_WRITE_LIBS} threadpool cacheutils dbbc processor)

install(TARGETS PrimProc DESTINATION ${ENGINE_BINDIR} COMPONENT platform)


