
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

add_executable(PrimProc ${PrimProc_SRCS})

add_dependencies(PrimProc loggingcpp)

target_link_libraries(PrimProc ${ENGINE_LDFLAGS} ${NETSNMP_LIBRARIES} ${ENGINE_WRITE_LIBS} threadpool cacheutils dbbc processor)

install(TARGETS PrimProc DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)


