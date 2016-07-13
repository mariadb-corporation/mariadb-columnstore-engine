
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(trapHandler_SRCS trapHandler.cpp)

add_executable(trapHandler ${trapHandler_SRCS})

target_link_libraries(trapHandler ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS})

install(TARGETS trapHandler DESTINATION ${ENGINE_BINDIR})

