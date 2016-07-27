
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(editem_SRCS editem.cpp)

add_executable(editem ${editem_SRCS})

add_dependencies(editem libnetsnmpmibs)

target_link_libraries(editem ${ENGINE_LDFLAGS} ${ENGINE_EXEC_LIBS})

install(TARGETS editem DESTINATION ${ENGINE_BINDIR})

