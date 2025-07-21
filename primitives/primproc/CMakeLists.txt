include_directories(${ENGINE_COMMON_INCLUDES} ../blockcache ../linux-port)

# ########## next target ###############

set(PrimProc_SRCS
    activestatementcounter.cpp
    batchprimitiveprocessor.cpp
    bppseeder.cpp
    bppsendthread.cpp
    columncommand.cpp
    command.cpp
    dictstep.cpp
    femsghandler.cpp
    filtercommand.cpp
    logger.cpp
    passthrucommand.cpp
    primitiveserver.cpp
    primproc.cpp
    pseudocc.cpp
    rssmonfcn.cpp
    rtscommand.cpp
    samenodepseudosocket.cpp
    serviceexemgr.cpp
    sqlfrontsessionthread.cpp
    umsocketselector.cpp
    ../../utils/common/crashtrace.cpp
)

columnstore_executable(PrimProc ${PrimProc_SRCS})

target_include_directories(PrimProc PRIVATE ${Boost_INCLUDE_DIRS})
columnstore_link(
    PrimProc
    ${ENGINE_LDFLAGS}
    ${ENGINE_WRITE_LIBS}
    threadpool
    cacheutils
    dbbc
    processor
    loggingcpp
    statistics_manager
)
