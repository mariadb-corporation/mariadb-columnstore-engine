
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(DMLProc_SRCS
    dmlproc.cpp
    dmlprocessor.cpp
    dmlresultbuffer.cpp
    batchinsertprocessor.cpp)

add_executable(DMLProc ${DMLProc_SRCS})

target_link_libraries(DMLProc ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${NETSNMP_LIBRARIES} threadpool ddlcleanuputil batchloader)

install(TARGETS DMLProc DESTINATION ${ENGINE_BINDIR} COMPONENT platform)



