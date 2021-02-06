
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(columnstoreSupport_SRCS columnstoreSupport.cpp mcsSupportUtil.cpp)

add_executable(columnstoreSupport ${columnstoreSupport_SRCS})

target_compile_options(columnstoreSupport PRIVATE -Wno-unused-result)

target_link_libraries(columnstoreSupport ${ENGINE_LDFLAGS} ncurses  ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS})

install(TARGETS columnstoreSupport DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)

install(PROGRAMS dbmsReport.sh bulklogReport.sh configReport.sh
    hardwareReport.sh logReport.sh resourceReport.sh
    DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)
              
