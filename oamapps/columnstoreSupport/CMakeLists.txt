
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(columnstoreSupport_SRCS columnstoreSupport.cpp)

add_executable(columnstoreSupport ${columnstoreSupport_SRCS})

target_link_libraries(columnstoreSupport ${ENGINE_LDFLAGS} readline ncurses  ${MARIADB_CLIENT_LIBS} ${ENGINE_EXEC_LIBS})

install(TARGETS columnstoreSupport DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

install(PROGRAMS alarmReport.sh bulklogReport.sh configReport.sh hadoopReport.sh 
    hardwareReport.sh logReport.sh resourceReport.sh softwareReport.sh 
    DESTINATION ${ENGINE_BINDIR} COMPONENT platform)
              
