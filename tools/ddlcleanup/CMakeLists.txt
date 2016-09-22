
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(ddlcleanup_SRCS ddlcleanup.cpp)

add_executable(ddlcleanup ${ddlcleanup_SRCS})

add_dependencies(ddlcleanup libnetsnmpmibs)

target_link_libraries(ddlcleanup ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ddlcleanuputil)

install(TARGETS ddlcleanup DESTINATION ${ENGINE_BINDIR} COMPONENT platform)

