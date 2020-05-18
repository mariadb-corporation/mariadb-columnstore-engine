
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############

set(idbmeminfo_SRCS idbmeminfo.cpp)

add_executable(idbmeminfo ${idbmeminfo_SRCS})

target_link_libraries(idbmeminfo ${ENGINE_LDFLAGS})

install(TARGETS idbmeminfo DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)

