
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(dbbuilder_SRCS dbbuilder.cpp systemcatalog.cpp)

add_executable(dbbuilder ${dbbuilder_SRCS})

add_dependencies(dbbuilder libnetsnmpmibs)

target_link_libraries(dbbuilder ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS})

install(TARGETS dbbuilder DESTINATION ${ENGINE_BINDIR})

