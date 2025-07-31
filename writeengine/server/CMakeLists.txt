include_directories(${ENGINE_COMMON_INCLUDES})

# ########## next target ###############

set(WriteEngineServer_SRCS
    we_server.cpp
    we_readthread.cpp
    we_ddlcommandproc.cpp
    we_observer.cpp
    we_dataloader.cpp
    we_brmrprtparser.cpp
    we_dmlcommandproc.cpp
    we_cleartablelockcmd.cpp
    we_cpifeederthread.cpp
    we_getfilesizes.cpp
    ../../utils/common/crashtrace.cpp
    ../../datatypes/mcs_datatype.cpp
)

columnstore_executable(WriteEngineServer ${WriteEngineServer_SRCS})

add_dependencies(WriteEngineServer loggingcpp)

columnstore_link(
    WriteEngineServer ${ENGINE_LDFLAGS} ${NETSNMP_LIBRARIES} ${ENGINE_WRITE_LIBS} threadpool writeengineredistribute
)
