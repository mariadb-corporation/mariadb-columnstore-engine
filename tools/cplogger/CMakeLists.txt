
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(cplogger_SRCS main.cpp)

add_executable(cplogger ${cplogger_SRCS})

target_link_libraries(cplogger ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS})

install(TARGETS cplogger DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)

