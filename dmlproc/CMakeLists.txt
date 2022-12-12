
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(DMLProc_SRCS
    dmlproc.cpp
    dmlprocessor.cpp
    dmlresultbuffer.cpp
    batchinsertprocessor.cpp
    ../utils/common/crashtrace.cpp)

add_executable(DMLProc ${DMLProc_SRCS})

add_dependencies(DMLProc loggingcpp)

target_link_libraries(DMLProc ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${NETSNMP_LIBRARIES} threadpool ddlcleanuputil batchloader)

if (WITH_CUSTOM_JEMALLOC)
   target_link_libraries(DMLProc custom_jemalloc_lib)
endif()

install(TARGETS DMLProc DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)



