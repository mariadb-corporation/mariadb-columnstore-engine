
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(clearShm_SRCS main.cpp)

add_executable(clearShm ${clearShm_SRCS})

target_link_libraries(clearShm ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS})

install(TARGETS clearShm DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)

