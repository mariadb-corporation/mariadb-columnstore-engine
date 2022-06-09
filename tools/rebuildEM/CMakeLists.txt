include_directories(${ENGINE_COMMON_INCLUDES})

set(rebuildEM_SRCS main.cpp rebuildEM.cpp)
add_executable(mcsRebuildEM ${rebuildEM_SRCS})
target_link_libraries(mcsRebuildEM ${ENGINE_LDFLAGS} ${ENGINE_WRITE_LIBS} ${MARIADB_CLIENT_LIBS} Boost::system Boost::filesystem)
install(TARGETS mcsRebuildEM DESTINATION ${ENGINE_BINDIR} COMPONENT columnstore-engine)
