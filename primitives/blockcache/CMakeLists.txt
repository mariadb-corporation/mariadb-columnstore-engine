include_directories(${ENGINE_COMMON_INCLUDES} ../primproc)

# ########## next target ###############

set(dbbc_STAT_SRCS
    blockcacheclient.cpp
    blockrequestprocessor.cpp
    fileblockrequestqueue.cpp
    filebuffer.cpp
    filebuffermgr.cpp
    filerequest.cpp
    iomanager.cpp
    stats.cpp
    fsutils.cpp
)
columnstore_static_library(dbbc ${dbbc_STAT_SRCS})
columnstore_link(dbbc ${NETSNMP_LIBRARIES} loggingcpp)
