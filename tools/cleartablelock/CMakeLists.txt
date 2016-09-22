
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(cleartablelock_SRCS cleartablelock.cpp cleartablelockthread.cpp)

add_executable(cleartablelock ${cleartablelock_SRCS})

add_dependencies(cleartablelock libnetsnmpmibs)

target_link_libraries(cleartablelock ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS})

install(TARGETS cleartablelock DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

