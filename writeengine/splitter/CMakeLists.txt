
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(cpimport_SRCS
    we_splitterapp.cpp
    we_cmdargs.cpp
    we_sdhandler.cpp
    we_respreadthread.cpp
    we_filereadthread.cpp
    we_splclient.cpp
    we_brmupdater.cpp
    we_tablelockgrabber.cpp
    we_xmlgetter.cpp)

add_executable(cpimport ${cpimport_SRCS})

target_link_libraries(cpimport ${ENGINE_LDFLAGS} ${NETSNMP_LIBRARIES} ${MARIADB_CLIENT_LIBS} ${ENGINE_WRITE_LIBS} batchloader threadpool)

install(TARGETS cpimport DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

