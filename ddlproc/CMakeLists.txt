
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(DDLProc_SRCS ddlproc.cpp ddlprocessor.cpp)

add_executable(DDLProc ${DDLProc_SRCS})

target_link_libraries(DDLProc ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} threadpool)

install(TARGETS DDLProc DESTINATION ${ENGINE_BINDIR})

