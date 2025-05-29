include_directories(${ENGINE_COMMON_INCLUDES} ${S3API_DIR})

# ########## next target ###############

set(cpimport_SRCS
    we_splitterapp.cpp
    we_cmdargs.cpp
    we_sdhandler.cpp
    we_respreadthread.cpp
    we_filereadthread.cpp
    we_splclient.cpp
    we_brmupdater.cpp
    we_tablelockgrabber.cpp
    we_xmlgetter.cpp
)

columnstore_executable(cpimport ${cpimport_SRCS})

columnstore_link(
    cpimport
    ${ENGINE_LDFLAGS}
    ${ENGINE_WRITE_LIBS}
    batchloader
    threadpool
    marias3
    boost_program_options
    loggingcpp
)
