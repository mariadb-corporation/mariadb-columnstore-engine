include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(DMLProc_SRCS dmlproc.cpp dmlprocessor.cpp dmlresultbuffer.cpp batchinsertprocessor.cpp
                 ../utils/common/crashtrace.cpp
)

columnstore_executable(DMLProc ${DMLProc_SRCS})

columnstore_link(
    DMLProc
    ${ENGINE_LDFLAGS}
    ${ENGINE_WRITE_LIBS}
    ${NETSNMP_LIBRARIES}
    threadpool
    ddlcleanuputil
    batchloader
    loggingcpp
)
