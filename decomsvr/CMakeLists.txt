
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(DecomSvr_SRCS quicklz.c server.cpp)

add_executable(DecomSvr ${DecomSvr_SRCS})

target_link_libraries(DecomSvr ${ENGINE_LDFLAGS} ${Boost_LIBRARIES} pthread rt)

install(TARGETS DecomSvr DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

