
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(sendtrap_SRCS sendtrap.cpp)

add_executable(sendtrap ${sendtrap_SRCS})

target_link_libraries(sendtrap ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS})

install(TARGETS sendtrap DESTINATION ${ENGINE_BINDIR})

